package com.hbase.controller;

import com.hbase.service.AppService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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

}
