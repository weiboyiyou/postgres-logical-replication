package com.haben.pgreplication;

import com.haben.pgreplication.config.DatabaseReplicationConfig;
import com.haben.pgreplication.config.SysConstants;
import com.haben.pgreplication.config.TaskConfig;
import com.haben.pgreplication.entity.DatabaseReplication;
import com.haben.pgreplication.ha.HaRegister;
import com.haben.pgreplication.listener.ReplicationLeaderSelectorListener;
import com.haben.pgreplication.util.PropertiesUtils;
import com.haben.pgreplication.zk.ZkClient;
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
			new ThreadPoolExecutor(SysConstants.POOL_SIZE, SysConstants.POOL_SIZE,
					5, TimeUnit.SECONDS,
					new ArrayBlockingQueue<>(SysConstants.POOL_SIZE),
					new ThreadFactory() {
						AtomicInteger atomicInteger = new AtomicInteger();

						@Override
						public Thread newThread(Runnable r) {
							Thread t = new Thread(r, "pg-rep-pool-" + atomicInteger.incrementAndGet());
							t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
								@Override
								public void uncaughtException(Thread t, Throwable e) {
									log.error("异常了{}", e);
								}
							});
							return t;
						}
					});

	public static void main(String[] args) throws Exception {
		System.out.println("PG-LOGICAL-RELIPCATION  is starting......");
		Thread.sleep(666);

		DatabaseReplicationConfig.initZkPath();
		HaRegister.syncExecTaskSizeToZk();

		LeaderSelector leaderSelector = new LeaderSelector(ZkClient.CLIENT, SysConstants.LEADER_PATH, new ReplicationLeaderSelectorListener());

		leaderSelector.start();
		leaderSelector.autoRequeue();

		while (true) {
			int activeCount = executor.getActiveCount();
//			taskcount.set(activeCount);
			int maximumPoolSize = executor.getMaximumPoolSize();
			log.debug("当前执行数量{},当前剩余数量:{}", activeCount, (maximumPoolSize - activeCount));
			// 写入zk 任务过来时候排序是否最小 最小的话那就执行 不然不执行
			log.debug("我是谁:{}", SysConstants.MACHINE_CODE);
//			HaRegister.syncExecTaskSizeToZk();
			Thread.sleep(20000);
			HaRegister.loadBalance();
		}
//		Thread.sleep(1000000);
//		boolean leader = leaderSelector.getLeader().isLeader();
//		log.debug("我是不是leader:" + leader);
	}


	public static void executorExec(TaskConfig config) {
		log.debug("线程池添加任务了");
		executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					DatabaseReplication databaseReplication = new DatabaseReplication(config);
					HaRegister.interruptList.add(databaseReplication);
					databaseReplication.replicateLogicalLog();
				} catch (Throwable e) {
					try {
						log.error(e.getMessage());
						Thread.sleep(5000);
						Thread.currentThread().interrupt();
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				}
			}
		});
	}

}
