package com.hbase.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.stereotype.Component;

import com.hbase.model.User;
import com.hbase.util.AppConstant;

@Component	
public class AppService {
	
	@Autowired
	private HbaseTemplate hbaseTemplate;
	
	
	public void createTables(){
		List<String> tables = new ArrayList<String>();
		for (String name : tables) {
			AppConstant.createUserTable(name);
		}
	}
	
	public List<User> getAllUser(){
		
		List<String> rows = hbaseTemplate.find("User", "name", new RowMapper<String>() {
			@Override
			public String mapRow(Result result, int rowNum) throws Exception {
				return result.toString();
			}
		});
		System.out.println("getAllUser >>>>>>>>>>>>>>>>>>>>>>>> END"+rows);
		for (String row : rows) {
			System.out.println("Printing row:" + row);
		}
		
		return null;
	}

	
}
