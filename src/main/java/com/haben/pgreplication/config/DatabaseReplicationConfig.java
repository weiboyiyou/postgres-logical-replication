package com.haben.pgreplication.config;

import com.haben.pgreplication.zk.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-07 03:30
 * @Version: 1.0
 **/
public class DatabaseReplicationConfig {

	public static void initZkPath() {
		try {
			ZkClient.client.delete().deletingChildrenIfNeeded().forPath(SysConstants.DB_TASK_PATH);//重置任务
			Stat leaderPath = ZkClient.client.checkExists().forPath(SysConstants.LEADER_PATH);
			Stat dbTaskPath = ZkClient.client.checkExists().forPath(SysConstants.DB_TASK_PATH);
			Stat doingTaskPath = ZkClient.client.checkExists().forPath(SysConstants.DOING_TASK_PATH);
			Stat nodeStatusPath = ZkClient.client.checkExists().forPath(SysConstants.NODE_STATUS_PATH);

			if (leaderPath == null) {
				ZkClient.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(SysConstants.LEADER_PATH);
			}
			if (dbTaskPath == null) {
				ZkClient.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(SysConstants.DB_TASK_PATH);
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
				ZkClient.client.create().withMode(CreateMode.PERSISTENT).forPath(SysConstants.DB_TASK_PATH + "/db1", db1.getBytes());
				ZkClient.client.create().withMode(CreateMode.PERSISTENT).forPath(SysConstants.DB_TASK_PATH + "/db2", db2.getBytes());
			}
			if (doingTaskPath == null) {
				ZkClient.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(SysConstants.DOING_TASK_PATH);
			}
			if (nodeStatusPath == null) {
				ZkClient.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(SysConstants.NODE_STATUS_PATH);

			}

//			client.create().withMode(CreateMode.PERSISTENT).forPath(LEADER_PATH);
//			client.create().withMode(CreateMode.PERSISTENT).forPath(DB_TASK_PATH);
//			client.create().withMode(CreateMode.PERSISTENT).forPath(DOING_TASK_PATH);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
