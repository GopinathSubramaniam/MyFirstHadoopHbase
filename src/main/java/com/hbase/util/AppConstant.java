package com.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@PropertySource("classpath:application.properties")
public class AppConstant {

	private static Logger logger = LoggerFactory.getLogger(AppConstant.class);

	private static TableSchema schema = null;
	private static Configuration config = null;

	private static String hbaseMaster = null;
	private static String zookeeperClientPort = null;
	private static String zookeeperQuorum = null;

	@Autowired
	public AppConstant(@Value("${hbase.master}") String hbaseMaster, @Value("${hbase.zookeeper.property.clientPort}") String zookeeperClientPort,
					   @Value("${hbase.zookeeper.quorum}") String zookeeperQuorum){ // Empty Constructor with Arguments
		this.hbaseMaster = hbaseMaster;
		this.zookeeperClientPort = zookeeperClientPort;
		this.zookeeperQuorum = zookeeperQuorum;
	}

	public static String createHbaseTable(String tableName){
		logger.info("CreateHBaseTable >>>>>>>>>> ");
		boolean tableExists = isTableExists(tableName);

		List<String> columnNames = new ArrayList<String>();//user table columns
		columnNames.add("name");
		columnNames.add("email");
		
		if(!tableExists){
			logger.info("Table "+tableName+ " is available >>>>>>>>>> ");
			List<TableSchema> tableSchemas = createSchema(columnNames);
			tableName = createTable("User", tableSchemas);
		}else{
			logger.info("Table "+tableName+ " is already exists >>>>>>>>>> ");
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
		logger.info("HBase Master URL >>>>>>>>> "+hbaseMaster);

		boolean isMasterExists = (config != null ? (config.get("hbase.master") == hbaseMaster) : false);
		
		if(!isMasterExists){
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
