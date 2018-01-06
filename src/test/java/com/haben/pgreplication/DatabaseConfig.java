package com.haben.pgreplication;

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
		String[] split = config.split("\n");// 0 url 1user 2passwd 3slotname
		this.dbUrl = getProperty(split[0]);
		this.user = getProperty(split[1]);
		this.passwd = getProperty(split[2]);
		this.slotName = getProperty(split[3]);
		this.taskName = getProperty(split[4]);
	}
	public String getProperty(String str){
		return str.split("=")[1];
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
