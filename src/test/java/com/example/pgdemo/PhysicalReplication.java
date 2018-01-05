package com.example.pgdemo;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-04 23:28
 * @Version: 1.0
 **/
public class PhysicalReplication {
	public static void main(String[] args) throws SQLException, InterruptedException {
		String url = "jdbc:postgresql://localhost:5432/pg";
		Properties props = new Properties();
		PGProperty.USER.set(props, "hpym365");
		PGProperty.PASSWORD.set(props, "postgres");
		PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "10.1");
		PGProperty.REPLICATION.set(props, "database");
		PGProperty.PREFER_QUERY_MODE.set(props, "simple");

		Connection con = DriverManager.getConnection(url, props);
		PGConnection replConnection = con.unwrap(PGConnection.class);

//		replConnection
//		replConnection.getReplicationAPI()
//				.createReplicationSlot()
//				.logical()
//				.withSlotName("demo_logical_slot")
//				.withOutputPlugin("test_decoding")
//				.make();

		//some changes after create replication slot to demonstrate receive it
//		con.setAutoCommit(true);
//		Statement st = con.createStatement();
//		st.execute("insert into test_logic_table(name) values('first tx changes')");
//		st.close();
//
//		st = con.createStatement();
//		st.execute("update test_logic_table set name = 'second tx change' where pk = 1");
//		st.close();
//
//		st = con.createStatement();
//		st.execute("delete from test_logic_table where pk = 1");
//		st.close();

		PGReplicationStream stream =
				replConnection.getReplicationAPI()
						.replicationStream()
						.logical()
						.withSlotName("demo_logical_slot")
						.withSlotOption("include-xids", false)
						.withSlotOption("skip-empty-xacts", true)
						.withStatusInterval(20, TimeUnit.SECONDS)
						.start();

		while (true) {
			//non blocking receive message
			ByteBuffer msg = stream.readPending();

			if (msg == null) {
				TimeUnit.MILLISECONDS.sleep(10L);
				continue;
			}

			int offset = msg.arrayOffset();
			byte[] source = msg.array();
			int length = source.length - offset;
			String command = new String(source, offset, length);

			if(command.startsWith("table")){
				System.out.println(command);
				String[] split = command.split(": ");
//
//				String tbName  = split[0].split(" ")[1];
//				System.out.println("tbName:"+tbName);
//				String option = split[1];
//				System.out.println("option:"+option);
//				switch (option){
//					case "DELETE":{
//						String[] splitDel = split[2].split(":");
//						String pkFelidAndType = splitDel[0];
//						int f = pkFelidAndType.indexOf("[");
//						String pkFeild = pkFelidAndType.substring(0,f);
//						String pkType = pkFelidAndType.substring(f+1,pkFelidAndType.length()-1);
//
//						String pkValue=splitDel[1];
//						System.out.println("pkFeild:"+pkFeild);
//						System.out.println("pkType:"+pkType);
//						System.out.println("pkValue:"+pkValue);
//					}
//				}
				StringBuffer sb = new StringBuffer();
				int count=1;
				for (String s : split) {
					System.out.println("首先打印样子:"+s);
					char[] chars = s.toCharArray();
					for(int i=0;i<chars.length;i++){
						if(chars[i]!=' '){
							sb.append(chars[i]);
						}else {
							System.out.println("sb:"+sb.toString());
							sb=new StringBuffer();
						}
						if(i==(chars.length-1)){
							System.out.println("sb:"+sb.toString());
							sb=new StringBuffer();
						}
					}
//					for (char aChar : chars) {
//						if(aChar!=' '){
//							sb.append(aChar);
//						}else {
//							System.out.println("sb:"+sb.toString());
//							sb=new StringBuffer();
//						}
//					}
				}
			}
//			System.out.println(command);

			//feedback
			stream.setAppliedLSN(stream.getLastReceiveLSN());
			stream.setFlushedLSN(stream.getLastReceiveLSN());
		}
	}
}
