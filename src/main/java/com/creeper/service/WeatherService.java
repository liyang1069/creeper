package com.creeper.service;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.apiproject.teardowall.CreeperService;
import com.creeper.domain.entity.Weather;


@Component
@Transactional
public class WeatherService extends BaseService implements CreeperService {
	
	public Weather catchWeather(String urlString) throws DocumentException{
		URL url = null;
		try {
			url = new URL(urlString);
		} catch (MalformedURLException e) {
			System.out.println("URL ERROR:" + urlString);
		}
		//创建SAXReader对象
        SAXReader reader = new SAXReader();
        //读取文件 转换成Document
        Document document = reader.read(url);
        //获取根节点元素对象
        Element root = document.getRootElement();
        System.out.println(root.getName());
        for ( @SuppressWarnings("unchecked")
		Iterator<Element> i = root.elementIterator(); i.hasNext(); ) {
            Element element = i.next();
            Weather weather = element2Weather(element);
            System.out.println(weather.getCityname());
            if("济南市".equals(weather.getCityname())){
            	return weather;
            }
        }
        return null;
	}
	
	private Weather element2Weather(Element element){
		Weather weather = null;
		if(element == null){
			return weather;
		}
		weather = new Weather();
		weather.setCityname(element.attributeValue("cityname"));
		weather.setHighTem(Integer.parseInt(element.attributeValue("tem1")));
		weather.setLowTem(Integer.parseInt(element.attributeValue("tem2")));
		weather.setNowTem(Integer.parseInt(element.attributeValue("temNow")));
		weather.setWindState(element.attributeValue("windState"));
		weather.setHumidity(element.attributeValue("humidity"));
		return weather;
	}

	@Override
	public String sayHello(String name) {
		return "Jerry say hello to " + name;
	}
}
