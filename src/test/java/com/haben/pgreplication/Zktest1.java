package com.haben.pgreplication;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-06 11:16
 * @Version: 1.0
 **/
public class Zktest1 {


	public static void main(String[] args) throws Exception {
		final AtomicInteger taskcount = new AtomicInteger();
		String url = "127.0.0.1:2181";
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client =
				CuratorFrameworkFactory.newClient(
						url,
						5000,
						3000,
						retryPolicy);

		client.start();
		LeaderSelector leaderSelector = new LeaderSelector(client, "/ld", new LeaderSelectorListener() {

			@Override
			public void stateChanged(CuratorFramework client, ConnectionState newState) {
				System.out.println("stateChangedstateChangedstateChanged");
			}

			@Override
			public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
				if(taskcount.get()<2){
					List<String> taskList = client.getChildren().forPath("/dbtask");
					List<String> doList = client.getChildren().forPath("/doing");
					Set taskSet = new HashSet<>(taskList);
					taskList.removeAll(doList);
					if(taskList.size()>0){
						String s = taskList.get(0);
						client.create().withMode(CreateMode.EPHEMERAL).forPath("/doing/"+s,"zktest111".getBytes());
					}else {
						System.out.println("任务没有了啊！！！");
					}
				}
				System.out.println("takeLeadership   "+new Date());
				Thread.sleep(10000);
			}
		});
		leaderSelector.start();
		leaderSelector.autoRequeue();
		Thread.sleep(1000000);
		boolean leader = leaderSelector.getLeader().isLeader();
		System.out.println("我是不是leader:" + leader);
	}

}
