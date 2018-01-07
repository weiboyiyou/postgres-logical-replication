package com.haben.pgreplication.listener;

import com.haben.pgreplication.DatabaseReplicationMain;
import com.haben.pgreplication.config.DatabaseConfig;
import com.haben.pgreplication.config.SysConstants;
import com.haben.pgreplication.ha.HaRegister;
import com.haben.pgreplication.zk.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-07 03:44
 * @Version: 1.0
 **/
public class ReplicationLeaderSelectorListener implements LeaderSelectorListener {

	private static final Logger log = LoggerFactory.getLogger(ReplicationLeaderSelectorListener.class);

	private static final AtomicInteger count = new AtomicInteger();
	@Override
	public void stateChanged(CuratorFramework client, ConnectionState newState) {
		log.debug("stateChanged");
	}

	@Override
	public void takeLeadership(CuratorFramework curatorFramework) {
		System.out.println("第"+count.incrementAndGet()+"次获取到了权限");
		log.debug("tk开始,当前执行的数量为:" + SysConstants.TASK_COUNT.get());
		if (SysConstants.TASK_COUNT.get() < SysConstants.POOL_SIZE
				&& SysConstants.MACHINE_CODE.equals(HaRegister.getMinExecTaskHost())) {
			log.debug("当前线程池还有空 并且 本机是执行数量最小的host");
			try {
				List<String> taskList = ZkClient.getChildList(SysConstants.DB_TASK_PATH);
				log.debug(taskList.toString());
				List<String> doList = ZkClient.getChildList(SysConstants.DOING_TASK_PATH);
				taskList.removeAll(doList);
				log.debug(taskList.toString());
				if (taskList.size() > 0) {
					String childDataPath = taskList.get(0);
					String dbTaskConfig = ZkClient.getNodeData(SysConstants.DB_TASK_PATH + "/" + childDataPath);
					DatabaseConfig config = new DatabaseConfig(dbTaskConfig);
//						String[] config = .split("\n");// 0 url 1user 2passwd 3slotname
					DatabaseReplicationMain.executorExec(config);
					ZkClient.client.create().withMode(CreateMode.EPHEMERAL).forPath(SysConstants.DOING_TASK_PATH + "/" + childDataPath, SysConstants.MACHINE_CODE.getBytes());
					SysConstants.TASK_COUNT.incrementAndGet();
					HaRegister.writeExecTaskSizeToZk();
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
}
