package com.example.pgdemo;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.common.protocol.types.Field;
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
	public static final AtomicInteger taskcount = new AtomicInteger(0);

	private static final String ROOT_PATH = "/pg-logic-replication";
	private static final String LEADER_PATH = ROOT_PATH + "/leader";
	private static final String DB_TASK_PATH = ROOT_PATH + "/db-task";
	public static final String DOING_TASK_PATH = ROOT_PATH + "/doing-task";
	private static final String NODE_STATUS_PATH = ROOT_PATH + "/node-status";

	public static List<DatabaseReplication> interruptList = new ArrayList<>();


	public static String getHost() {
		try {
			return InetAddress.getLocalHost().getHostName();
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
			client.delete().deletingChildrenIfNeeded().forPath(DB_TASK_PATH);//重置任务
			Stat leaderPath = client.checkExists().forPath(LEADER_PATH);
			Stat dbTaskPath = client.checkExists().forPath(DB_TASK_PATH);
			Stat doingTaskPath = client.checkExists().forPath(DOING_TASK_PATH);
			Stat nodeStatusPath = client.checkExists().forPath(NODE_STATUS_PATH);

			if (leaderPath == null) {
				client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(LEADER_PATH);
			}
			if (dbTaskPath == null) {
				client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(DB_TASK_PATH);
				String db1 = "dburl=jdbc:postgresql://localhost:5432/pg\n" +
						"user=hpym365\n" +
						"password=hpym365\n" +
						"slotname=demo_logical_slot21\n" +
						"taskname=db1";
				String db2 = "dburl=jdbc:postgresql://localhost:5432/pg\n" +
						"user=hpym365\n" +
						"password=hpym365\n" +
						"slotname=demo_logical_slot\n" +
						"taskname=db2";
				// 目录名字和taskname名字要一样不然会有问题
				client.create().withMode(CreateMode.PERSISTENT).forPath(DB_TASK_PATH + "/db1", db1.getBytes());
				client.create().withMode(CreateMode.PERSISTENT).forPath(DB_TASK_PATH + "/db2", db2.getBytes());
			}
			if (doingTaskPath == null) {
				client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(DOING_TASK_PATH);
			}
			if (nodeStatusPath == null) {
				client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(NODE_STATUS_PATH);

			}

//			client.create().withMode(CreateMode.PERSISTENT).forPath(LEADER_PATH);
//			client.create().withMode(CreateMode.PERSISTENT).forPath(DB_TASK_PATH);
//			client.create().withMode(CreateMode.PERSISTENT).forPath(DOING_TASK_PATH);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public static void main(String[] args) throws Exception {
		CuratorFramework client = getClient();
		client.start();

		initZkPath();
		syncExecTaskSizeToZk();
		LeaderSelector leaderSelector = new LeaderSelector(Zktest.client, LEADER_PATH, new LeaderSelectorListener() {

			@Override
			public void stateChanged(CuratorFramework client, ConnectionState newState) {
				System.out.println("stateChangedstateChangedstateChanged");
			}

			@Override
			public void takeLeadership(CuratorFramework curatorFramework) {
				System.out.println("tk开始,当前执行的数量为:" + taskcount.get());
				if (taskcount.get() < 2 && MACHINE_CODE.equals(getMinExecHost())) {
					System.out.println("当前线程池还有空 并且 本机是执行数量最小的host");
					try {
						List<String> taskList = client.getChildren().forPath(DB_TASK_PATH);
						System.out.println(taskList);
						List<String> doList = client.getChildren().forPath(DOING_TASK_PATH);
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
							Thread.sleep(5000);
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
//			taskcount.set(activeCount);
			int maximumPoolSize = executor.getMaximumPoolSize();
			System.out.println("当前执行数量" + activeCount);
			System.out.println("当前剩余数量" + (maximumPoolSize - activeCount));
			InetAddress localHost = InetAddress.getLocalHost();
			// 写入zk 任务过来时候排序是否最小 最小的话那就执行 不然不执行
			System.out.println("我是谁:" + MACHINE_CODE);
			syncExecTaskSizeToZk();
			Thread.sleep(20000);
			loadBalance();
		}
//		Thread.sleep(1000000);
//		boolean leader = leaderSelector.getLeader().isLeader();
//		System.out.println("我是不是leader:" + leader);
	}

	public static List<String> getDbTask() {
		try {
			return client.getChildren().forPath(DB_TASK_PATH);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	// 找到最小执行任务的主机
	public static String getMinExecHost() {
		try {
			List<String> nodes = client.getChildren().forPath(NODE_STATUS_PATH);
			int minSize = Integer.MAX_VALUE;
			String minNode = null;
			for (String node : nodes) {
				byte[] bytes = client.getData().forPath(NODE_STATUS_PATH + "/" + node);
				int size = Integer.parseInt(new String(bytes));
				if (size < minSize) {
					minSize = size;
					minNode = node;
				}
			}
			System.out.println("getMinExecHost:" + minNode);
			return minNode;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void syncExecTaskSizeToZk() {
		try {
			// 写入当前节点执行的replication数量
			Stat stat = client.checkExists().forPath(NODE_STATUS_PATH + "/" + MACHINE_CODE);
			byte[] data = (taskcount.get() + "").getBytes();
			if (stat == null) {
				client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(NODE_STATUS_PATH + "/" + MACHINE_CODE, data);
			} else {
				client.setData().forPath(NODE_STATUS_PATH + "/" + MACHINE_CODE, data);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void loadBalance() {

		try {
			// 所有节点
			List<String> nodes = client.getChildren().forPath(LEADER_PATH);
			List<String> dbTask = getDbTask();

			int max = (dbTask.size() / nodes.size()) + (dbTask.size() % nodes.size() == 0 ? 0 : 1);//没个点最大应该执行的数量

			System.out.println("max===" + max);
			if (taskcount.get() > max) {
				System.out.println("当前节点干的活太多了，有好多节点比他少，停一个吧");
				interruptTask();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void interruptTask() {
		if (interruptList.size() > 0) {
			DatabaseReplication databaseReplication = interruptList.get(0);
			databaseReplication.interruptReplication();
			interruptList.remove(databaseReplication);
			System.out.println("终止一下试试哈");
		} else {
			System.out.println("没有可终止的任务");
		}
	}

	public static void executorExec(DatabaseConfig config) {
		System.out.println("线程池添加任务了");
		executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					DatabaseReplication databaseReplication = new DatabaseReplication();
					interruptList.add(databaseReplication);
					databaseReplication.read(config, client);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

}
