package com.creeper.domain.entity;

import java.util.Date;

public class Weather extends BaseModel {
	//<city cityX="303" cityY="279" cityname="济南市" centername="济南市" fontColor="FFFF00" pyName="" state1="0" state2="0" 
	//stateDetailed="晴" tem1="14" tem2="6" temNow="13" windState="南风3-4级" windDir="西南风" windPower="2级" humidity="30%" 
	//time="14:55" url="101120101"/>
	
	private String cityname;
	private int highTem;
	private int lowTem;
	private int nowTem;
	private String windState;
	private String humidity;
	private Date time;
	
	public String getCityname() {
		return cityname;
	}
	public void setCityname(String cityname) {
		this.cityname = cityname;
	}
	public int getHighTem() {
		return highTem;
	}
	public void setHighTem(int highTem) {
		this.highTem = highTem;
	}
	public int getLowTem() {
		return lowTem;
	}
	public void setLowTem(int lowTem) {
		this.lowTem = lowTem;
	}
	public int getNowTem() {
		return nowTem;
	}
	public void setNowTem(int nowTem) {
		this.nowTem = nowTem;
	}
	public String getWindState() {
		return windState;
	}
	public void setWindState(String windState) {
		this.windState = windState;
	}
	public String getHumidity() {
		return humidity;
	}
	public void setHumidity(String humidity) {
		this.humidity = humidity;
	}
	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
}
