package com.haben.pgreplication.config;

import com.haben.pgreplication.util.PropertiesUtils;
import com.haben.pgreplication.zk.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-07 03:30
 * @Version: 1.0
 **/
public class DatabaseReplicationConfig {


	public static void initZkPath() {
		try {

			Stat leaderPath = ZkClient.client.checkExists().forPath(SysConstants.LEADER_PATH);
			Stat dbTaskPath = ZkClient.client.checkExists().forPath(SysConstants.DB_TASK_PATH);
			if (dbTaskPath != null) {
				//每次启动重置任务  看看上线时候要不要删除掉
				ZkClient.client.delete().deletingChildrenIfNeeded().forPath(SysConstants.DB_TASK_PATH);
				dbTaskPath = null;
			}
			Stat doingTaskPath = ZkClient.client.checkExists().forPath(SysConstants.DOING_TASK_PATH);
			Stat nodeStatusPath = ZkClient.client.checkExists().forPath(SysConstants.NODE_STATUS_PATH);

			if (leaderPath == null) {
				ZkClient.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(SysConstants.LEADER_PATH);
			}
			if (dbTaskPath == null) {
				ZkClient.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(SysConstants.DB_TASK_PATH);
				Map<String, String> exectaskList = getExectaskList();
				exectaskList.forEach((taskName, taskConfig) -> {
					try {
						ZkClient.client.create().withMode(CreateMode.PERSISTENT).forPath(SysConstants.DB_TASK_PATH + "/" + taskName, taskConfig.getBytes());
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
			}
			if (doingTaskPath == null) {
				ZkClient.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(SysConstants.DOING_TASK_PATH);
			}
			if (nodeStatusPath == null) {
				ZkClient.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(SysConstants.NODE_STATUS_PATH);

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	private static final String EXEC_TASK = "exectask";
	private static final String DBTASK_PREFIX = "dbtask.";
	private static final String DBTASK_DBURL = "dburl";
	private static final String DBTASK_USER = "user";
	private static final String DBTASK_PASSWORD = "password";
	private static final String DBTASK_SLOTNAME = "slotname";
	private static final String DBTASK_TASKNAME = "taskname";
	private static final String DELIMITER = ".";
	private static final String NEW_LINE = "\n";


	public static Map<String, String> getExectaskList() {
		String execTask = PropertiesUtils.getProperty(EXEC_TASK);
		String[] tasks = execTask.split(",");
		Map<String, String> resMap = new HashMap();
		for (String task : tasks) {
			String dburl = PropertiesUtils.getProperty(DBTASK_PREFIX + task + DELIMITER + DBTASK_DBURL) + NEW_LINE;
			String user = PropertiesUtils.getProperty(DBTASK_PREFIX + task + DELIMITER + DBTASK_USER) + NEW_LINE;
			String password = PropertiesUtils.getProperty(DBTASK_PREFIX + task + DELIMITER + DBTASK_PASSWORD) + NEW_LINE;
			String slotname = PropertiesUtils.getProperty(DBTASK_PREFIX + task + DELIMITER + DBTASK_SLOTNAME) + NEW_LINE;
			String taskname = PropertiesUtils.getProperty(DBTASK_PREFIX + task + DELIMITER + DBTASK_TASKNAME);
			resMap.put(PropertiesUtils.getProperty(DBTASK_PREFIX + task + DELIMITER + DBTASK_TASKNAME), dburl + user + password + slotname + taskname);
		}
		System.out.println(resMap);
		return resMap;
	}
}
