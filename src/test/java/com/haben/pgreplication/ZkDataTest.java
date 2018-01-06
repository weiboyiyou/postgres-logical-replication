package com.haben.pgreplication;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-06 13:41
 * @Version: 1.0
 **/
public class ZkDataTest {
	public static void main(String[] args) throws Exception {

		String url = "127.0.0.1:2181";
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client =
				CuratorFrameworkFactory.newClient(
						url,
						5000,
						3000,
						retryPolicy);

		client.start();

		byte[] bytes = client.getData().forPath("/dbtask/db1");

		String res = new String(bytes);
		String[] split = res.split("\n");
		for (String s : split) {
			System.out.println("ssss:"+s);
		}
		System.out.println(res);
	}
}
