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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


	private static Logger log = LoggerFactory.getLogger(Zktest.class);

	private static List<DatabaseReplication> interruptList = new ArrayList<>();


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
									log.debug("异常了" + e);
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
			client.delete().deletingChildrenIfNeeded().forPath(SysConstans.DB_TASK_PATH);//重置任务
			Stat leaderPath = client.checkExists().forPath(SysConstans.LEADER_PATH);
			Stat dbTaskPath = client.checkExists().forPath(SysConstans.DB_TASK_PATH);
			Stat doingTaskPath = client.checkExists().forPath(SysConstans.DOING_TASK_PATH);
			Stat nodeStatusPath = client.checkExists().forPath(SysConstans.NODE_STATUS_PATH);

			if (leaderPath == null) {
				client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(SysConstans.LEADER_PATH);
			}
			if (dbTaskPath == null) {
				client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(SysConstans.DB_TASK_PATH);
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
				client.create().withMode(CreateMode.PERSISTENT).forPath(SysConstans.DB_TASK_PATH + "/db1", db1.getBytes());
				client.create().withMode(CreateMode.PERSISTENT).forPath(SysConstans.DB_TASK_PATH + "/db2", db2.getBytes());
			}
			if (doingTaskPath == null) {
				client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(SysConstans.DOING_TASK_PATH);
			}
			if (nodeStatusPath == null) {
				client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(SysConstans.NODE_STATUS_PATH);

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
		LeaderSelector leaderSelector = new LeaderSelector(Zktest.client, SysConstans.LEADER_PATH, new LeaderSelectorListener() {

			@Override
			public void stateChanged(CuratorFramework client, ConnectionState newState) {
				log.debug("stateChangedstateChangedstateChanged");
			}

			@Override
			public void takeLeadership(CuratorFramework curatorFramework) {
				log.debug("tk开始,当前执行的数量为:" + SysConstans.TASK_COUNT.get());
				if (SysConstans.TASK_COUNT.get() < 2 && SysConstans.MACHINE_CODE.equals(getMinExecHost())) {
					log.debug("当前线程池还有空 并且 本机是执行数量最小的host");
					try {
						List<String> taskList = client.getChildren().forPath(SysConstans.DB_TASK_PATH);
						log.debug(taskList.toString());
						List<String> doList = client.getChildren().forPath(SysConstans.DOING_TASK_PATH);
						taskList.removeAll(doList);
						log.debug(taskList.toString());
						if (taskList.size() > 0) {
							String childDataPath = taskList.get(0);
							byte[] bytes = Zktest.client.getData().forPath(SysConstans.DB_TASK_PATH + "/" + childDataPath);
							DatabaseConfig config = new DatabaseConfig(new String(bytes));
//						String[] config = .split("\n");// 0 url 1user 2passwd 3slotname
							Zktest.executorExec(config);
							Zktest.client.create().withMode(CreateMode.EPHEMERAL).forPath(SysConstans.DOING_TASK_PATH + "/" + childDataPath, SysConstans.MACHINE_CODE.getBytes());
							SysConstans.TASK_COUNT.incrementAndGet();
						} else {
							log.debug("任务没有了啊,可以休息休息了！！！");
							Thread.sleep(5000);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else {
					log.debug("takeLeadership   " + new Date());
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
			log.debug("当前执行数量" + activeCount);
			log.debug("当前剩余数量" + (maximumPoolSize - activeCount));
			InetAddress localHost = InetAddress.getLocalHost();
			// 写入zk 任务过来时候排序是否最小 最小的话那就执行 不然不执行
			log.debug("我是谁:" + SysConstans.MACHINE_CODE);
			syncExecTaskSizeToZk();
			Thread.sleep(20000);
			loadBalance();
		}
//		Thread.sleep(1000000);
//		boolean leader = leaderSelector.getLeader().isLeader();
//		log.debug("我是不是leader:" + leader);
	}

	public static List<String> getDbTask() {
		try {
			return client.getChildren().forPath(SysConstans.DB_TASK_PATH);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	// 找到最小执行任务的主机
	public static String getMinExecHost() {
		try {
			List<String> nodes = client.getChildren().forPath(SysConstans.NODE_STATUS_PATH);
			int minSize = Integer.MAX_VALUE;
			String minNode = null;
			for (String node : nodes) {
				byte[] bytes = client.getData().forPath(SysConstans.NODE_STATUS_PATH + "/" + node);
				int size = Integer.parseInt(new String(bytes));
				if (size < minSize) {
					minSize = size;
					minNode = node;
				}
			}
			log.debug("getMinExecHost:" + minNode);
			return minNode;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void syncExecTaskSizeToZk() {
		try {
			// 写入当前节点执行的replication数量
			Stat stat = client.checkExists().forPath(SysConstans.NODE_STATUS_PATH + "/" + SysConstans.MACHINE_CODE);
			byte[] data = String.valueOf(SysConstans.TASK_COUNT.get()).getBytes();

			if (stat == null) {
				client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(SysConstans.NODE_STATUS_PATH + "/" + SysConstans.MACHINE_CODE, data);
			} else {
				client.setData().forPath(SysConstans.NODE_STATUS_PATH + "/" + SysConstans.MACHINE_CODE, data);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void loadBalance() {

		try {
			// 所有节点
			List<String> nodes = client.getChildren().forPath(SysConstans.LEADER_PATH);
			List<String> dbTask = getDbTask();

			int max = (dbTask.size() / nodes.size()) + (dbTask.size() % nodes.size() == 0 ? 0 : 1);//每个点 最大应该执行的数量

			log.debug("max===" + max);
			if (SysConstans.TASK_COUNT.get() > max) {
				log.debug("当前节点干的活太多了 执行数量为:" + SysConstans.TASK_COUNT.get() + "，应该不超过:" + max + "...有好多节点比他少，停一个吧");
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
			log.debug("终止一下试试哈");
		} else {
			log.debug("没有可终止的任务");
		}
	}

	public static void executorExec(DatabaseConfig config) {
		log.debug("线程池添加任务了");
		executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					DatabaseReplication databaseReplication = new DatabaseReplication(client, config);
					interruptList.add(databaseReplication);
					databaseReplication.replicateLogicalLog();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

}
