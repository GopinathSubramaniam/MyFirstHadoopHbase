package com.hbase.service;

import java.io.IOException;
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

@Component	
public class AppService {
	
	@Autowired
	private HbaseTemplate hbaseTemplate;
	
	
	public void createTables(){
		String tableName="User";
		TableName tableNameVerified = TableName.valueOf(tableName);
		HTableDescriptor table = new HTableDescriptor(tableNameVerified);
		String name = "name";
		String email = "email";
		String mobile = "mobile";
		
		HColumnDescriptor nameColumnFamily = new HColumnDescriptor(name.getBytes());
		HColumnDescriptor emailColumnFamily = new HColumnDescriptor(email.getBytes());
		HColumnDescriptor mobileColumnFamily = new HColumnDescriptor(mobile.getBytes());
		
		table.addFamily(nameColumnFamily);
		table.addFamily(emailColumnFamily);
		table.addFamily(mobileColumnFamily);
		
		try {
			System.out.println("hbase.master >>>>>>>>>>>>>>>> "+hbaseTemplate.getConfiguration().get("hbase.master"));
			HBaseAdmin hBaseAdmin = new HBaseAdmin(hbaseTemplate.getConfiguration());
			hBaseAdmin.createTable(table);
			hBaseAdmin.close();
			System.out.println("USER TABLE CREATED");
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
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
