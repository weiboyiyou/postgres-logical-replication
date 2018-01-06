package com.example.pgdemo;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-06 11:16
 * @Version: 1.0
 **/
public class Zktest {

	private static final String MACHINE_CODE = UUID.randomUUID().toString() + ":" + getHost();

	private static final String ROOT_PATH = "/pg-logic-replication";
	private static final String LEADER_PATH = ROOT_PATH + "/leader";
	private static final String DB_TASK_PATH = ROOT_PATH + "/db-task";
	private static final String DOING_TASK_PATH = ROOT_PATH + "/doing-task";

	public static String getHost() {
		try {
			return InetAddress.getLocalHost().toString();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return "unknow";
	}

	public static ThreadPoolExecutor executor =
			new ThreadPoolExecutor(2, 2,
					5, TimeUnit.SECONDS,
					new ArrayBlockingQueue<>(10),
					new ThreadFactory() {
						AtomicInteger atomicInteger = new AtomicInteger();

						@Override
						public Thread newThread(Runnable r) {
							Thread t = new Thread(r, "name" + atomicInteger.incrementAndGet());
							t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
								@Override
								public void uncaughtException(Thread t, Throwable e) {
									System.out.println("异常了" + e);
								}
							});
							return t;
						}
					});

	private static CuratorFramework client = null;

	public static CuratorFramework getClient() {
		if (client == null) {
			synchronized (Zktest.class) {
				if (client == null) {
					String url = "127.0.0.1:2181";
					RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
					client =
							CuratorFrameworkFactory.newClient(
									url,
									5000,
									3000,
									retryPolicy);

				}
			}
		}
		return client;
	}

	public static void initZkPath() {

		try {
			Stat leaderPath = client.checkExists().forPath(LEADER_PATH);
			Stat dbTaskPath = client.checkExists().forPath(DB_TASK_PATH);
			Stat doingTaskPath = client.checkExists().forPath(DOING_TASK_PATH);
			if (leaderPath == null) {
				client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(LEADER_PATH);
			}
			if (dbTaskPath == null) {
				client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(DB_TASK_PATH);
				String db1 = "dburl=jdbc:postgresql://localhost:5432/pg\n" +
						"user=hpym365\n" +
						"password=hpym365\n" +
						"slotname=demo_logical_slot21";
				String db2 = "dburl=jdbc:postgresql://localhost:5432/pg\n" +
						"user=hpym365\n" +
						"password=hpym365\n" +
						"slotname=demo_logical_slot";
				client.create().withMode(CreateMode.PERSISTENT).forPath(DB_TASK_PATH + "/db1", db1.getBytes());
				client.create().withMode(CreateMode.PERSISTENT).forPath(DB_TASK_PATH + "/db2", db2.getBytes());
			}
			if (doingTaskPath == null) {
				client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(DOING_TASK_PATH);
			}

//			client.create().withMode(CreateMode.PERSISTENT).forPath(LEADER_PATH);
//			client.create().withMode(CreateMode.PERSISTENT).forPath(DB_TASK_PATH);
//			client.create().withMode(CreateMode.PERSISTENT).forPath(DOING_TASK_PATH);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public static void main(String[] args) throws Exception {
		final AtomicInteger taskcount = new AtomicInteger();
		CuratorFramework client = getClient();
		client.start();

		initZkPath();

		LeaderSelector leaderSelector = new LeaderSelector(Zktest.client, LEADER_PATH, new LeaderSelectorListener() {

			@Override
			public void stateChanged(CuratorFramework client, ConnectionState newState) {
				System.out.println("stateChangedstateChangedstateChanged");
			}

			@Override
			public void takeLeadership(CuratorFramework curatorFramework) {
				System.out.println("tk开始");
				if (taskcount.get() < 2) {
					try {
						List<String> taskList = Zktest.client.getChildren().forPath(DB_TASK_PATH);
						System.out.println(taskList);
						List<String> doList = Zktest.client.getChildren().forPath(DOING_TASK_PATH);
						taskList.removeAll(doList);
						System.out.println(taskList);
						if (taskList.size() > 0) {
							String childDataPath = taskList.get(0);
							byte[] bytes = Zktest.client.getData().forPath(DB_TASK_PATH + "/" + childDataPath);
							DatabaseConfig config = new DatabaseConfig(new String(bytes));
//						String[] config = .split("\n");// 0 url 1user 2passwd 3slotname
							Zktest.executorExec(config);
							Zktest.client.create().withMode(CreateMode.EPHEMERAL).forPath(DOING_TASK_PATH + "/" + childDataPath, MACHINE_CODE.getBytes());
							taskcount.incrementAndGet();
						} else {
							System.out.println("任务没有了啊,可以休息休息了！！！");
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else {
					System.out.println("takeLeadership   " + new Date());
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		});
		leaderSelector.start();
		leaderSelector.autoRequeue();

		while (true) {
			int activeCount = executor.getActiveCount();
			int maximumPoolSize = executor.getMaximumPoolSize();
			System.out.println("当前执行数量" + activeCount);
			System.out.println("当前剩余数量" + (maximumPoolSize - activeCount));
			InetAddress localHost = InetAddress.getLocalHost();
			// 写入zk 任务过来时候排序是否最小 最小的话那就执行 不然不执行
			System.out.println("我是谁:" + MACHINE_CODE);
			Thread.sleep(20000);
		}
//		Thread.sleep(1000000);
//		boolean leader = leaderSelector.getLeader().isLeader();
//		System.out.println("我是不是leader:" + leader);
	}

	public static void executorExec(DatabaseConfig config) {
		System.out.println("线程池添加任务了");
		executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					DatabaseReplication.read(config);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

}
