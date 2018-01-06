package com.example.pgdemo;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-05 23:16
 * @Version: 1.0
 **/
public class DatabaseReplication {
	public static ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 5, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100000), new ThreadFactory() {
		AtomicInteger atomicInteger = new AtomicInteger();

		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r, "name" + atomicInteger.incrementAndGet());
			t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
				@Override
				public void uncaughtException(Thread t, Throwable e) {
					System.out.println("异常了" + e);
				}
			});
			return t;
		}
	});

	public static void main(String[] args) throws Exception {

		executor.execute(new Runnable() {
			@Override
			public void run() {
				DatabaseReplication at = new DatabaseReplication();
				try {
//					at.read();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});


	}

	public static void read(DatabaseConfig config) throws Exception {
		String url = "jdbc:postgresql://localhost:5432/pg";
		Properties props = new Properties();
		PGProperty.USER.set(props, config.getUser());
		PGProperty.PASSWORD.set(props, config.getPasswd());
		PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "10.1");
		PGProperty.REPLICATION.set(props, "database");
		PGProperty.PREFER_QUERY_MODE.set(props, "simple");

		Connection con = null;
		try {
			con = DriverManager.getConnection(config.getDbUrl(), props);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		PGConnection
				replConnection = con.unwrap(PGConnection.class);


		PGReplicationStream stream = replConnection.getReplicationAPI()
				.replicationStream()
				.logical()
				.withSlotName(config.getSlotName())
				.start();

		while (true) {
			ByteBuffer msg = null;
			try {
				msg = stream.read();
			} catch (SQLException e) {
				e.printStackTrace();
			}


			if (msg == null) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println(new Date());
				continue;
			}
			int offset = msg.arrayOffset();
			byte[] source = msg.array();
			int length = source.length - offset;
			String command = new String(source, offset, length);
			if(command.startsWith("table")){
				System.out.println(command + "   slot:" + config.getSlotName());
			}

			stream.setAppliedLSN(stream.getLastReceiveLSN());
			stream.setFlushedLSN(stream.getLastReceiveLSN());

		}
	}
}
