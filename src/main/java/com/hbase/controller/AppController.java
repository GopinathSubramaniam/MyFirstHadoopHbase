package com.hbase.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.hbase.service.AppService;

@RestController
@RequestMapping("/app")
public class AppController {
	
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
