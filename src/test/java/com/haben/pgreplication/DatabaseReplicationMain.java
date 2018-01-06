package com.haben.pgreplication;

import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class DatabaseReplicationMain {


	private static Logger log = LoggerFactory.getLogger(DatabaseReplicationMain.class);


	private static ThreadPoolExecutor executor =
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


	public static void main(String[] args) throws Exception {

		DatabaseReplicationConfig.initZkPath();
		HaRegister.syncExecTaskSizeToZk();

		LeaderSelector leaderSelector = new LeaderSelector(ZkClient.client, SysConstants.LEADER_PATH, new ReplicationLeaderSelectorListener());

		leaderSelector.start();
		leaderSelector.autoRequeue();

		while (true) {
			int activeCount = executor.getActiveCount();
//			taskcount.set(activeCount);
			int maximumPoolSize = executor.getMaximumPoolSize();
			log.debug("当前执行数量" + activeCount);
			log.debug("当前剩余数量" + (maximumPoolSize - activeCount));
			// 写入zk 任务过来时候排序是否最小 最小的话那就执行 不然不执行
			log.debug("我是谁:" + SysConstants.MACHINE_CODE);
//			HaRegister.syncExecTaskSizeToZk();
			Thread.sleep(20000);
			HaRegister.loadBalance();
		}
//		Thread.sleep(1000000);
//		boolean leader = leaderSelector.getLeader().isLeader();
//		log.debug("我是不是leader:" + leader);
	}


	public static void executorExec(DatabaseConfig config) {
		log.debug("线程池添加任务了");
		executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					DatabaseReplication databaseReplication = new DatabaseReplication(ZkClient.client, config);
					HaRegister.interruptList.add(databaseReplication);
					databaseReplication.replicateLogicalLog();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

}
