package com.haben.pgreplication.util;

import java.io.*;
import java.util.Properties;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-07 03:23
 * @Version: 1.0
 **/
public class PropertiesUtils {
	private static Properties properties = null;

	private static final String FILE_PATH = "src/main/resources/application.properties";

	private static Properties getProperties() {
		InputStream in = null;
		try {
			in = new BufferedInputStream(new FileInputStream(FILE_PATH));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		Properties p = new Properties();
		try {
			p.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return p;
	}

	public static String getProperty(String key) {
		if (properties == null) {
			synchronized (PropertiesUtils.class) {
				properties = getProperties();
			}
		}
		String res = "";
		try{
			res = properties.getProperty(key).trim();
		}catch (Exception e){
			throw new RuntimeException("读取参数错误请检查配置,参数:"+key+"不存在."+e);
		}
		return res;
	}

	public static void main(String[] args) {
		Properties properties = getProperties();
		System.out.println(properties);
		String property = properties.getProperty("taska");
		System.out.println(property);
	}



}
