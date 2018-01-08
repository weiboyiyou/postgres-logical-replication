package com.haben.pgreplication.entity;

import com.haben.pgreplication.config.SysConstants;
import com.haben.pgreplication.config.TaskConfig;
import com.haben.pgreplication.ha.HaRegister;
import com.haben.pgreplication.kafka.MessageProducer;
import com.haben.pgreplication.zk.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


	private static final Logger log = LoggerFactory.getLogger(DatabaseReplication.class);

	private volatile boolean interrupt = false;
	private CuratorFramework client = null;
	private TaskConfig config = null;
	private Connection connection = null;
	private PGReplicationStream stream = null;
	private MessageProducer producer = null;

	public void interruptReplication() {
		this.interrupt = true;
	}

	public DatabaseReplication(TaskConfig config) throws SQLException {
		this.client = ZkClient.getClient();
		this.config = config;
		this.connection = dbConnection();
		producer = new MessageProducer(config);
	}

	private Connection dbConnection() throws SQLException {
		//"jdbc:postgresql://localhost:5432/pg";
		String url = config.getDbUrl();
		Properties props = new Properties();
		PGProperty.USER.set(props, config.getUser());
		PGProperty.PASSWORD.set(props, config.getPasswd());
		PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "10.1");
		PGProperty.REPLICATION.set(props, "database");
		PGProperty.PREFER_QUERY_MODE.set(props, "simple");

		return DriverManager.getConnection(url, props);
	}

	public void replicateLogicalLog() throws Exception {
		// 创建任务doing
		String s = client.create().withMode(CreateMode.EPHEMERAL).forPath(SysConstants.DOING_TASK_PATH + "/" + config.getTaskName(), SysConstants.MACHINE_CODE.getBytes());
		HaRegister.incrementTaskSizeAndWriteToZk();
		//同步成功之后释放await
		config.getCountDownLatch().countDown();
		read();
		interruptReplicate();
	}

	private void read() {
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
//				System.out.println("interruptinterrupt:"+interrupt    +"     "+Thread.currentThread().getName());
				//read non block..
				msg = stream.readPending();
				if (msg == null) {
					Thread.sleep(200);
					continue;
				}
				int offset = msg.arrayOffset();
				byte[] source = msg.array();
				int length = source.length - offset;
				String command = new String(source, offset, length);
				LogSequenceNumber lastReceiveLSN = stream.getLastReceiveLSN();
				int hashCode = (offset + command).hashCode();
//				if (command.startsWith("table")) {
				producer.sendMsg(config.getTopic(), lastReceiveLSN.asLong(), command);
				System.out.println(command + "   slot:" + config.getSlotName());
//				}

				stream.setAppliedLSN(lastReceiveLSN);
				stream.setFlushedLSN(lastReceiveLSN);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stream != null) {
					stream.close();
				}
				if (connection != null) {
					connection.close();
				}
				if (client != null) {
					client.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}


	}

	private void interruptReplicate() throws Exception {
		//任务数量-1
		int i = HaRegister.decrementTaskSizeAndWriteToZk();
		log.debug("被终止了wakaka,当前线程执行数量为" + i);
		// 删除doing 因为进程没终止 这个还会在的  修改之后不删除进程挂掉  client结束应该自动删除了
//		client.delete().forPath(SysConstants.DOING_TASK_PATH + "/" + config.getTaskName());
		// 修改当前节点的数量
//		HaRegister.writeExecTaskSizeToZk();
	}
}
