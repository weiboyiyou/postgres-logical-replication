package com.haben.pgreplication.config;

import com.haben.pgreplication.util.PropertiesUtils;
import com.haben.pgreplication.zk.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private static final Logger log = LoggerFactory.getLogger(DatabaseReplicationConfig.class);

	public static void initZkPath() {
		try {

			Stat leaderPath = ZkClient.CLIENT.checkExists().forPath(SysConstants.LEADER_PATH);
			Stat dbTaskPath = ZkClient.CLIENT.checkExists().forPath(SysConstants.DB_TASK_PATH);
			if (dbTaskPath != null) {
				//每次启动重置任务  看看上线时候要不要删除掉
				log.debug("dbTaskPath不为空,删除重置任务.....");
				ZkClient.CLIENT.delete().deletingChildrenIfNeeded().forPath(SysConstants.DB_TASK_PATH);
				dbTaskPath = null;
			}
			Stat doingTaskPath = ZkClient.CLIENT.checkExists().forPath(SysConstants.DOING_TASK_PATH);
			Stat nodeStatusPath = ZkClient.CLIENT.checkExists().forPath(SysConstants.NODE_STATUS_PATH);

			if (leaderPath == null) {
				ZkClient.CLIENT.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(SysConstants.LEADER_PATH);
			}
			if (dbTaskPath == null) {
				ZkClient.CLIENT.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(SysConstants.DB_TASK_PATH);
				Map<String, String> exectaskList = getExecTaskList();
				exectaskList.forEach((taskName, taskConfig) -> {
					try {
						ZkClient.CLIENT.create().withMode(CreateMode.PERSISTENT).forPath(SysConstants.DB_TASK_PATH + "/" + taskName, taskConfig.getBytes());
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
			}
			if (doingTaskPath == null) {
				ZkClient.CLIENT.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(SysConstants.DOING_TASK_PATH);
			}
			if (nodeStatusPath == null) {
				ZkClient.CLIENT.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(SysConstants.NODE_STATUS_PATH);

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	private static final String EXEC_TASK = "exectask";
	private static final String TASK_PREFIX = "dbtask.";
	private static final String TASK_DBURL = "dburl";
	private static final String TASK_USER = "user";
	private static final String TASK_PASSWORD = "password";
	private static final String TASK_SLOTNAME = "slotname";
	private static final String TASK_TASKNAME = "taskname";
	private static final String TASK_TOPIC = "topic";
	private static final String TASK_KAFKAURL = "kafkaUrl";
	private static final String DELIMITER = ".";
	private static final String NEW_LINE = "\n";


	public static Map<String, String> getExecTaskList() {
		String execTask = PropertiesUtils.getProperty(EXEC_TASK);
		String[] tasks = execTask.split(",");
		Map<String, String> resMap = new HashMap();
		for (String task : tasks) {
			String dbUrl = PropertiesUtils.getProperty(TASK_PREFIX + task + DELIMITER + TASK_DBURL) + NEW_LINE;
			String user = PropertiesUtils.getProperty(TASK_PREFIX + task + DELIMITER + TASK_USER) + NEW_LINE;
			String password = PropertiesUtils.getProperty(TASK_PREFIX + task + DELIMITER + TASK_PASSWORD) + NEW_LINE;
			String slotName = PropertiesUtils.getProperty(TASK_PREFIX + task + DELIMITER + TASK_SLOTNAME) + NEW_LINE;
			String taskName = PropertiesUtils.getProperty(TASK_PREFIX + task + DELIMITER + TASK_TASKNAME)+ NEW_LINE;
			String topic = PropertiesUtils.getProperty(TASK_PREFIX + task + DELIMITER + TASK_TOPIC)+ NEW_LINE;
			String kafkaUrl = PropertiesUtils.getProperty(TASK_PREFIX + task + DELIMITER + TASK_KAFKAURL);
			if(resMap.get(PropertiesUtils.getProperty(TASK_PREFIX + task + DELIMITER + TASK_TASKNAME))!=null){
				throw new RuntimeException("application配置文件taskname重复了，请修改");
			}
			resMap.put(PropertiesUtils.getProperty(TASK_PREFIX + task + DELIMITER + TASK_TASKNAME), dbUrl + user + password + slotName + taskName+topic+kafkaUrl);
		}
		System.out.println(resMap);
		return resMap;
	}
}
