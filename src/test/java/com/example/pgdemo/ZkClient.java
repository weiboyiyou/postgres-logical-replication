package com.example.pgdemo;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.List;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-07 03:20
 * @Version: 1.0
 **/
public class ZkClient {

	private static final String ZK_URL = "zkurl";

	public final static CuratorFramework client = getClient();

	private static CuratorFramework getClient() {
		String url = PropertiesUtils.getProperty(ZK_URL);
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework curatorFramework =
				CuratorFrameworkFactory.newClient(
						url,
						5000,
						3000,
						retryPolicy);

		curatorFramework.start();
		return curatorFramework;
	}


	public static List<String> getChildList(String path) throws Exception {
		return ZkClient.client.getChildren().forPath(path);
	}

	public static String getNodeData(String path) throws Exception {
		return new String(ZkClient.client.getData().forPath(path));
	}

	public static void main(String[] args) {
		CuratorFramework client = getClient();
		System.out.println(client);
	}
}
