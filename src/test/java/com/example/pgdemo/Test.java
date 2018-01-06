package com.example.pgdemo;


import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-04 22:43
 * @Version: 1.0
 **/
public class Test {
	public static void main(String[] args) throws SQLException, InterruptedException {
		String url = "jdbc:postgresql://localhost:5432/pg";
		Properties props = new Properties();
		PGProperty.USER.set(props, "hpym365");
		PGProperty.PASSWORD.set(props, "pg");
		System.out.println(props);
		PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
		PGProperty.REPLICATION.set(props, "database");

		PGProperty.PREFER_QUERY_MODE.set(props, "simple");
//
		Connection con = DriverManager.getConnection(url, props);

		PGConnection replConnection = con.unwrap(PGConnection.class);
//		replConnection.getReplicationAPI()
//				.createReplicationSlot()
//				.logical()
//				.withSlotName("demo_logical_slot21")
//				.withOutputPlugin("test_decoding")
//				.make();
		PGReplicationStream stream =
				replConnection.getReplicationAPI()
						.replicationStream()
						.logical()
						.withSlotName("demo_logical_slot21")
						.withSlotOption("include-xids", false)
						.withSlotOption("skip-empty-xacts", true)
						.start();

		ByteBuffer msg = stream.read();
		int offset = msg.arrayOffset();
			byte[] source = msg.array();
			int length = source.length - offset;
			System.out.println(new String(source, offset, length));
//		System.out.println("read"+read);
//		while (true) {
//			//non blocking receive message
//			ByteBuffer msg = stream.readPending();
//
//			if (msg == null) {
//				TimeUnit.MILLISECONDS.sleep(10L);
//				continue;
//			}
//
//			int offset = msg.arrayOffset();
//			byte[] source = msg.array();
//			int length = source.length - offset;
//			System.out.println(new String(source, offset, length));
//		}

	}

}
