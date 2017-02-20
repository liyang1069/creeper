package com.creeper;

import java.net.URL;

public class Common {
	
	public static final String weatherRootUrl = "http://flash.weather.com.cn/wmaps/xml/jinan.xml";
	//public static final String weatherRootUrl = "http://www.weather.com.cn/data/sk/101010100.html";
	
	public static String getUrlContext(String urlString, String method){
		String resource = ""; 
		try { 
			URL url = new URL(urlString);
			java.net.HttpURLConnection conn = (java.net.HttpURLConnection)url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod(method);
			java.io.BufferedReader in = new java.io.BufferedReader(new java.io.InputStreamReader(conn.getInputStream(),"UTF-8"));
			String line;
			while ((line = in.readLine()) != null) {
				resource += line;
			}
			in.close();
		} catch (Exception e) {
			
		}
		return resource;
	}
	
}
