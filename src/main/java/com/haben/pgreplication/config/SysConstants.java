package com.haben.pgreplication.config;

import com.haben.pgreplication.util.PropertiesUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-07 01:27
 * @Version: 1.0
 **/
public class SysConstants {
	public static final String MACHINE_CODE = UUID.randomUUID().toString() + ":" + getHost();
	public static final AtomicInteger TASK_COUNT = new AtomicInteger(0);
	private static final String ROOT_PATH = "/pg-logical-replication";
	public static final String LEADER_PATH = ROOT_PATH + "/leader";
	public static final String DB_TASK_PATH = ROOT_PATH + "/db-task";
	public static final String DOING_TASK_PATH = ROOT_PATH + "/doing-task";
	public static final String NODE_STATUS_PATH = ROOT_PATH + "/node-status";
	public static final int POOL_SIZE = Integer.valueOf(PropertiesUtils.getProperty("pool-size"));

	private static String getHost() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return "unknow";
		}
	}
}
