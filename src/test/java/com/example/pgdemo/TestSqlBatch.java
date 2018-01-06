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
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-04 23:28
 * @Version: 1.0
 **/
public class TestSqlBatch {
	public static void main(String[] args) throws SQLException, InterruptedException {
		String url = "jdbc:postgresql://localhost:5432/pg";
		Properties props = new Properties();
		PGProperty.USER.set(props, "hpym365");
		PGProperty.PASSWORD.set(props, "postgres");
		PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "10.1");
		PGProperty.REPLICATION.set(props, "database");
		PGProperty.PREFER_QUERY_MODE.set(props, "simple");

		Connection con = DriverManager.getConnection(url, props);

		//some changes after create replication slot to demonstrate receive it
		con.setAutoCommit(true);
		int all = 10;
		long begin = System.currentTimeMillis();
		for(int k = 0;k<1;k++){
			for(int i=1;i<all;i++){
				Statement st = con.createStatement();
				st.execute("delete from  test where id="+i);
				st.close();
			}
			for(int i=1;i<all;i++){
				Statement statement = con.createStatement();
				String sql = "insert into test values("+i+",22)";
				System.out.println(sql);
				boolean execute = statement.execute(sql);

//			Statement st = con.createStatement();
//			boolean execute = st.execute("insert into test values( i,'wakaka')");
				System.out.println(execute);
				statement.close();
			}


//			Thread.sleep(500);
		}
		System.out.println(System.currentTimeMillis()-begin);
		System.out.println(new Date());

//		st = con.createStatement();
//		st.execute("update test_logic_table set name = 'second tx change' where pk = 1");
//		st.close();
//
//		st = con.createStatement();
//		st.execute("delete from test_logic_table where pk = 1");
//		st.close();


	}
}
