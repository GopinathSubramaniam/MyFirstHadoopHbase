package com.hbase.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
@PropertySource("classpath:application.properties")
public class AppConstant {

	private static TableSchema schema = null;
	private static Configuration config = null;
	
	@Autowired
	private static Environment env;
	
	public static String createUserTable(String tableName){
		boolean tableExists = isTableExists(tableName);
		
		List<String> columnNames = new ArrayList<String>();
		columnNames.add("name");
		columnNames.add("email");
		
		if(!tableExists){
			List<TableSchema> tableSchemas = createSchema(columnNames);
			tableName = createTable("User", tableSchemas);
		}else{
			System.out.println("Table Already Exists");
		}
		return tableName;
	}
	
	public static List<TableSchema> createSchema(List<String> columnNames){
		List<TableSchema> tableSchemas = new ArrayList<TableSchema>();
		for (String column : columnNames) {
			schema = new TableSchema();
			schema.setColumnName(column);
			tableSchemas.add(schema);
		}
		return tableSchemas;
	}
	
	public static String createTable(String tableName, List<TableSchema> tableSchemas){
		TableName tableNameVerified = TableName.valueOf(tableName);
		HTableDescriptor table = new HTableDescriptor(tableNameVerified);
		for (TableSchema tableSchema : tableSchemas) {
			HColumnDescriptor nameColumnFamily = new HColumnDescriptor(tableSchema.getColumnName());
			table.addFamily(nameColumnFamily);
		}
		Configuration config = getConfig();
		try {
			
			HBaseAdmin hBaseAdmin = new HBaseAdmin(config);
			hBaseAdmin.createTable(table);
			hBaseAdmin.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return table.getNameAsString();
	}
	
	public static Configuration getConfig(){
		String hbaseMaster = env.getProperty("hbase.master");
		String zookeeperClientPort = env.getProperty("hbase.zookeeper.property.clientPort");
		String zookeeperQuorum = env.getProperty("hbase.zookeeper.quorum");
		
		boolean isMasterExists = (config.get("hbase.master") == hbaseMaster);
		
		if(config  == null && !isMasterExists){
			config = new Configuration();
			config.set("hbase.master", hbaseMaster);
			config.set("hbase.zookeeper.property.clientPort", zookeeperClientPort);
			config.set("hbase.zookeeper.quorum", zookeeperQuorum);
		}
		return config;
	}
	
	public static boolean isTableExists(String tableName){
		TableName tableNameVerified = TableName.valueOf(tableName);
		Configuration config = getConfig();
		HBaseAdmin hBaseAdmin;
		boolean isTableExists = false;
		try {
			hBaseAdmin = new HBaseAdmin(config);
			isTableExists = hBaseAdmin.tableExists(tableNameVerified);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return isTableExists;
	}
	
}
