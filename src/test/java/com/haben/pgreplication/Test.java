package com.haben.pgreplication;

import com.haben.pgreplication.zk.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-08 下午2:20
 * @Version: 1.0
 **/
public class Test {
	public static void main(String[] args) throws Exception {


		CuratorFramework client = ZkClient.getClient();

		client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/123");

		Logger log = LoggerFactory.getLogger(Test.class);
		log.debug("123123什么鬼");
		System.out.println("已经创建了");
		Thread.sleep(5000);
		client.close();

		Thread.sleep(100000);



	}
}
