package com.haben.pgreplication.entity;

import com.haben.pgreplication.config.DatabaseConfig;
import com.haben.pgreplication.config.SysConstants;
import com.haben.pgreplication.ha.HaRegister;
import org.apache.curator.framework.CuratorFramework;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-05 23:16
 * @Version: 1.0
 **/
public class DatabaseReplication {


	private volatile boolean interrupt = false;
	private CuratorFramework client = null;
	private DatabaseConfig config = null;
	private Connection connection = null;
	private PGReplicationStream stream = null;

	public void interruptReplication() {
		this.interrupt = true;
	}

	public DatabaseReplication(CuratorFramework client, DatabaseConfig config) throws SQLException {
		this.client = client;
		this.config = config;
	}

	private Connection dbConnection() throws SQLException {
		String url = "jdbc:postgresql://localhost:5432/pg";
		Properties props = new Properties();
		PGProperty.USER.set(props, config.getUser());
		PGProperty.PASSWORD.set(props, config.getPasswd());
		PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "10.1");
		PGProperty.REPLICATION.set(props, "database");
		PGProperty.PREFER_QUERY_MODE.set(props, "simple");

		return DriverManager.getConnection(config.getDbUrl(), props);
	}

	public void replicateLogicalLog() throws Exception {
		this.connection = dbConnection();
		read();
		interruptReplicate();
	}

	private void read() throws Exception {
		try {
			PGConnection
					replConnection = connection.unwrap(PGConnection.class);


			stream = replConnection.getReplicationAPI()
					.replicationStream()
					.logical()
					.withSlotName(config.getSlotName())
					.start();
			ByteBuffer msg = null;
			while (true && !interrupt) {

				msg = stream.readPending();//read non block..
				if (msg == null) {
					Thread.sleep(200);
					continue;
				}
				int offset = msg.arrayOffset();
				byte[] source = msg.array();
				int length = source.length - offset;
				String command = new String(source, offset, length);
				if (command.startsWith("table")) {
					System.out.println(command + "   slot:" + config.getSlotName());
				}

				stream.setAppliedLSN(stream.getLastReceiveLSN());
				stream.setFlushedLSN(stream.getLastReceiveLSN());
			}
		} catch (
				Exception e)

		{
			e.printStackTrace();
		} finally

		{
			if (stream != null)
				stream.close();
			if (connection != null)
				connection.close();
		}


	}

	private void interruptReplicate() throws Exception {
		int i = SysConstants.TASK_COUNT.decrementAndGet();
		System.out.println("被终止了wakaka,当前线程执行数量为" + i);
		// 删除doing 因为进程没终止 这个还会在的
		client.delete().forPath(SysConstants.DOING_TASK_PATH + "/" + config.getTaskName());
		// 修改当前节点的数量
		HaRegister.writeExecTaskSizeToZk();
	}
}