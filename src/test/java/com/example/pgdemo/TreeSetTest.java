package com.example.pgdemo;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-06 23:00
 * @Version: 1.0
 **/
public class TreeSetTest {
	public static void main(String[] args) throws Exception {

		String str = "123:$:asdf";
		String[] split = str.split("\\:\\$\\:");
		for (String s : split) {
			System.out.println(s);
		}

		final String ROOT_PATH = "/pg-logic-replication";

		final String NODE_STATUS = ROOT_PATH + "/node-status";

		String url = "127.0.0.1:2181";
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client =
				CuratorFrameworkFactory.newClient(
						url,
						5000,
						3000,
						retryPolicy);

		TreeSet<String> treeSet = new TreeSet<>(new Comparator<String>() {
			@Override
			public int compare(String s1, String s2) {
//				System.out.println(s1+"   "+s2);
				String[] split1 = s1.split(":");
				String[] split2 = s2.split(":");
				if (Integer.valueOf(split1[1]) > Integer.valueOf(split2[1]))
					return 1;
				return -1;
			}
		});

		treeSet.add("123123:10");
		treeSet.add("123123:13");
		treeSet.add("123123:5");
		treeSet.add("123123:77");

//		List<String> nodes = client.getChildren().forPath(NODE_STATUS);
//		for (String node : nodes) {
//			byte[] bytes = client.getData().forPath(NODE_STATUS + "/" + node);
//			treeSet.add(new String(bytes));
//		}
		String first = treeSet.first();
		System.out.println(first);
	}
}
