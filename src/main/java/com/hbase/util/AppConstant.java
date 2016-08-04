package com.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@PropertySource("classpath:application.properties")
public class AppConstant {

	private static Logger logger = LoggerFactory.getLogger(AppConstant.class);

	private static TableSchema schema = null;
	private static Configuration hbaseConfig = null;
	private static Configuration hadoopConfig = null;

	@Autowired
	private Environment env;

	private static AppConstant appConstant = new AppConstant();

	public String createHbaseTable(String tableName){
		logger.info("CreateHBaseTable >>>>>>>>>> ");
		boolean tableExists = appConstant.isTableExists(tableName);

		List<String> columnNames = new ArrayList<String>();//user table columns
		columnNames.add("name");
		columnNames.add("email");
		
		if(!tableExists){
			logger.info("Table "+tableName+ " is available >>>>>>>>>> ");
			List<TableSchema> tableSchemas = appConstant.createSchema(columnNames);
			tableName = appConstant.createTable("User", tableSchemas);
		}else{
			logger.info("Table "+tableName+ " is already exists >>>>>>>>>> ");
		}
		return tableName;
	}
	
	public List<TableSchema> createSchema(List<String> columnNames){
		List<TableSchema> tableSchemas = new ArrayList<TableSchema>();
		for (String column : columnNames) {
			schema = new TableSchema();
			schema.setColumnName(column);
			tableSchemas.add(schema);
		}
		return tableSchemas;
	}
	
	public String createTable(String tableName, List<TableSchema> tableSchemas){
		TableName tableNameVerified = TableName.valueOf(tableName);
		HTableDescriptor table = new HTableDescriptor(tableNameVerified);
		for (TableSchema tableSchema : tableSchemas) {
			HColumnDescriptor nameColumnFamily = new HColumnDescriptor(tableSchema.getColumnName());
			table.addFamily(nameColumnFamily);
		}
		Configuration config = getHBaseConfig();
		try {
			
			HBaseAdmin hBaseAdmin = new HBaseAdmin(config);
			hBaseAdmin.createTable(table);
			hBaseAdmin.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return table.getNameAsString();
	}
	
	public Configuration getHadoopConfig(){
		String hdfsPath = env.getProperty("hdfs.path");
		String hadoopHomeDir = env.getProperty("hadoop.home.dir");

		logger.info("Hadoop Configuration Called. HDFS_PATH ::: "+hdfsPath);

		boolean isHadoopConfigExists = (hadoopConfig != null ? (hadoopConfig.get("fs.default.name") == hdfsPath) : false);
		
		if(!isHadoopConfigExists){
			hadoopConfig = new Configuration();
			logger.info("Hadoop Configuration Called. HDFS_PATH ::: "+hdfsPath);
			hadoopConfig.set("fs.default.name", hdfsPath);
			//hadoopConfig.set("hadoop.home.dir", hadoopHomeDir);
		}
		return hadoopConfig;
	}

	public Configuration getHBaseConfig(){
		String hbaseMaster = env.getProperty("hdfs.path");
		String zookeeperClientPort = env.getProperty("hbase.zookeeper.property.clientPort");
		String zookeeperQuorum = env.getProperty("hbase.zookeeper.quorum");

		logger.info("HBase Master URL >>>>>>>>> "+hbaseMaster);
		boolean isMasterExists = (hbaseConfig != null ? (hbaseConfig.get("hbase.master") == hbaseMaster) : false);

		if(!isMasterExists){
			hbaseConfig = new Configuration();
			hbaseConfig.set("hbase.master", hbaseMaster);
			hbaseConfig.set("hbase.zookeeper.property.clientPort", zookeeperClientPort);
			hbaseConfig.set("hbase.zookeeper.quorum", zookeeperQuorum);
		}
		return hbaseConfig;
	}
	
	public boolean isTableExists(String tableName){
		TableName tableNameVerified = TableName.valueOf(tableName);
		Configuration config = getHBaseConfig();
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
