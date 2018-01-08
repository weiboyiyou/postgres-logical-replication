package com.haben.pgreplication;

import com.haben.pgreplication.config.SysConstants;
import com.haben.pgreplication.config.TaskConfig;
import com.haben.pgreplication.zk.ZkClient;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018/1/9 03:22
 * @param:
 * @Return:
 **/
@SpringBootApplication
@RestController
public class PgLogicalApplication {

	public static void main(String[] args) {
		SpringApplication.run(PgLogicalApplication.class, args);
	}

	@RequestMapping("/task")
	public List getTaskList() throws Exception {
		final List<String> taskList = ZkClient.getChildList(SysConstants.DB_TASK_PATH);
		final List resList = new ArrayList();
		taskList.forEach(childDataPath -> {
			try {
				String taskConfigStr = ZkClient.getNodeData(SysConstants.DB_TASK_PATH + "/" + childDataPath);
				TaskConfig taskConfig = new TaskConfig(taskConfigStr);
				resList.add(taskConfig);
			} catch (Exception e) {
				e.printStackTrace();
			}

		});
		return resList;
	}


	@RequestMapping("/doing")
	public Map getDoingList() throws Exception {
		final List<String> doingList = ZkClient.getChildList(SysConstants.DOING_TASK_PATH);
		final Map<String, List<TaskConfig>> resMap = new HashMap();
		doingList.forEach(childDataPath -> {
			try {
				String taskConfigStr = ZkClient.getNodeData(SysConstants.DB_TASK_PATH + "/" + childDataPath);
				String machine = ZkClient.getNodeData(SysConstants.DOING_TASK_PATH + "/" + childDataPath);
				TaskConfig taskConfig = new TaskConfig(taskConfigStr);
				List<TaskConfig> taskConfigs = resMap.get(machine) == null ?
						taskConfigs = new ArrayList<>() : resMap.get(machine);
				taskConfigs.add(taskConfig);
				resMap.put(machine, taskConfigs);
			} catch (Exception e) {
				e.printStackTrace();
			}

		});
		return resMap;
	}


	@RequestMapping("/machine")
	public Map getMachineList() throws Exception {
		final List<String> doingList = ZkClient.getChildList(SysConstants.NODE_STATUS_PATH);
		final Map<String, String> resMap = new HashMap();
		doingList.forEach(node -> {
			try {
				String nodeTaskNum = ZkClient.getNodeData(SysConstants.NODE_STATUS_PATH + "/" + node);
				resMap.put(node, nodeTaskNum);
			} catch (Exception e) {
				e.printStackTrace();
			}

		});
		return resMap;
	}
}
