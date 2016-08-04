package com.hbase.service;

import com.hbase.model.User;
import com.hbase.util.AppConstant;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component	
public class AppService {

	private Logger logger = LoggerFactory.getLogger(AppService.class);

	@Autowired
	private HbaseTemplate hbaseTemplate;

	@Autowired
	private AppConstant appConstant;

	@Autowired
	private Environment env;

	public void createTables(){
		List<String> tables = new ArrayList<String>();
		tables.add("User"); //Adding table names
		for (String name : tables) {
			appConstant.createHbaseTable(name);
		}
	}
	
	public List<User> getAllUser(){
		
		List<String> rows = hbaseTemplate.find("User", "name", new RowMapper<String>() {
			@Override
			public String mapRow(Result result, int rowNum) throws Exception {
				return result.toString();
			}
		});
		logger.info("getAllUser >>>>>>>>>>>>>>>>>>>>>>>> END" + rows);
		for (String row : rows) {
			System.out.println("Printing row:" + row);
		}
		
		return null;
	}



	
}
