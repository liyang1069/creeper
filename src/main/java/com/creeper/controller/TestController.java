package com.creeper.controller;

import javax.annotation.Resource;

import org.dom4j.DocumentException;
//import org.springframework.web.bind.annotation.RequestMapping;
//import org.springframework.web.bind.annotation.RestController;

import com.creeper.Common;
import com.creeper.domain.entity.Weather;
import com.creeper.service.WeatherService;

//@RestController
public class TestController extends BaseController {
	
	@Resource
	private WeatherService weatherService;
	
//	@RequestMapping(value="/hello")
//	public String hello(){
//		return "fine! Thank you!";
//	}
//	
//	@RequestMapping(value="/weather")
//	public Weather catchWeather(){
//		Weather weather = null;
//		try {
//			weather = weatherService.catchWeather(Common.weatherRootUrl);
//		} catch (DocumentException e) {
//			System.out.println("ERROR");
//		}
//		return weather;
//	}
	
}
