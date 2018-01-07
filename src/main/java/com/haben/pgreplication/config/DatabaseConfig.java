package com.haben.pgreplication.config;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-06 13:58
 * @Version: 1.0
 **/
public class DatabaseConfig {

	private String dbUrl;
	private String user;
	private String passwd;
	private String slotName;
	private String taskName;

	public DatabaseConfig(String config) {
		// 0 url 1user 2passwd 3slotname
		String[] split = config.split("\n");
		this.dbUrl = split[0];
		this.user = split[1];
		this.passwd = split[2];
		this.slotName = split[3];
		this.taskName = split[4];
	}


	public String getDbUrl() {
		return dbUrl;
	}

	public void setDbUrl(String dbUrl) {
		this.dbUrl = dbUrl;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPasswd() {
		return passwd;
	}

	public void setPasswd(String passwd) {
		this.passwd = passwd;
	}

	public String getSlotName() {
		return slotName;
	}

	public void setSlotName(String slotName) {
		this.slotName = slotName;
	}

	public String getTaskName() {
		return taskName;
	}
}
