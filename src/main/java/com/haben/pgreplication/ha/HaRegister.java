package com.haben.pgreplication.ha;

import com.haben.pgreplication.config.SysConstants;
import com.haben.pgreplication.entity.DatabaseReplication;
import com.haben.pgreplication.zk.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-07 03:48
 * @Version: 1.0
 **/
public class HaRegister {
	private static final Logger log = LoggerFactory.getLogger(HaRegister.class);
	public final static List<DatabaseReplication> interruptList = new CopyOnWriteArrayList<>();


	// 找到最小执行任务数的主机
	public static String getMinExecTaskHost() {
		String minNode = SysConstants.MACHINE_CODE;
		try {
			List<String> nodes = ZkClient.getChildList(SysConstants.NODE_STATUS_PATH);
			int minSize = Integer.MAX_VALUE;
			for (String node : nodes) {
				String data = ZkClient.getNodeData(SysConstants.NODE_STATUS_PATH + "/" + node);
				int size = Integer.parseInt(data);
				if (size < minSize) {
					minSize = size;
					minNode = node;
				}
			}
			log.debug("当前最小执行节点为：getMinExecHost:{}", minNode);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return minNode;
	}

	public static void loadBalance() {

		try {
			// 所有节点
			List<String> nodes = ZkClient.getChildList(SysConstants.LEADER_PATH);
			List<String> dbTask = ZkClient.getChildList(SysConstants.DB_TASK_PATH);

			//每个点 最大应该执行的数量
			int max = (dbTask.size() / nodes.size()) + (dbTask.size() % nodes.size() == 0 ? 0 : 1);

			log.debug("每个节点应该执行的最大不超过:max==={}", max);
			if (SysConstants.TASK_COUNT.get() > max) {
				log.debug("当前节点干的活太多了 执行数量为:{},应该不超过:{}...有好多节点比他少，停一个吧", SysConstants.TASK_COUNT.get(), max);
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

	public static void syncExecTaskSizeToZk() {
		try {
			// 写入当前节点执行的replication数量
			Stat stat = ZkClient.CLIENT.checkExists().forPath(SysConstants.NODE_STATUS_PATH + "/" + SysConstants.MACHINE_CODE);

			if (stat == null) {
				createAndwriteExecTaskSizeToZk();
			} else {
				writeExecTaskSizeToZk();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void createAndwriteExecTaskSizeToZk() throws Exception {
		ZkClient.CLIENT.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(SysConstants.NODE_STATUS_PATH + "/" + SysConstants.MACHINE_CODE, String.valueOf(SysConstants.TASK_COUNT.get()).getBytes());

	}

	public static void writeTaskSizeToZk(int taskSize) throws Exception {
		ZkClient.CLIENT.setData().forPath(SysConstants.NODE_STATUS_PATH + "/" + SysConstants.MACHINE_CODE, String.valueOf(taskSize).getBytes());
	}

	public static void writeExecTaskSizeToZk() throws Exception {
		writeTaskSizeToZk(SysConstants.TASK_COUNT.get());
	}


	public static int decrementTaskSizeAndWriteToZk() throws Exception {
		int taskSize;
		writeTaskSizeToZk(taskSize = SysConstants.TASK_COUNT.decrementAndGet());
		return taskSize;
	}

	public static int incrementTaskSizeAndWriteToZk() throws Exception {
		int taskSize;
		// 增加任务数量
		writeTaskSizeToZk(taskSize = SysConstants.TASK_COUNT.incrementAndGet());
		return taskSize;
	}
}
