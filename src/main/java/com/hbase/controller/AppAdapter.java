package com.hbase.controller;

import com.hbase.service.AppService;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/app")
public class AppAdapter {
	
	@Autowired
	private AppService appService;
	
	@RequestMapping(value="/createTables")
	public void createTables(){
		System.out.println("AppController:createTables >>>>>>>>>>> ");
		appService.createTables();
	}
	
	@RequestMapping(value="/getAllUser")
	public boolean getAllUser(){
		appService.getAllUser();
		return true;
	}

	@RequestMapping(value="/uploadImage", method= RequestMethod.POST)
	public boolean uploadImage(@RequestParam("file") MultipartFile file, @RequestParam("user") String data) throws Exception{
		System.out.println(">>>>>>>>>> uploadImage >>>>>>>>>> File = " + file.getOriginalFilename());

		JSONObject userObj = new JSONObject(data);

		System.out.println(">>>>>>>>>> uploadImage >>>>>>>>>> User = " + userObj);
		if(file != null) appService.uploadMedia(file);
		return true;
	}
	
}
