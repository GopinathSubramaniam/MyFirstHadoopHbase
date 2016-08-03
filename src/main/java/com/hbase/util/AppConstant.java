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
	private static Configuration hbaseConfig = null;
	private static Configuration hadoopConfig = null;

	public static String HDFS_PATH = null;
	public static String HBASE_MASTER = null;
	public static String ZOOKEEPER_CLIENTPORT = null;
	public static String ZOOKEEPER_QUORUM = null;
	public static String HDFS_MEDIA_IMAGE_PATH = null;
	public static String HDFS_MEDIA_VIDEO_PATH = null;

	@Autowired
	public AppConstant(@Value("${hdfs.path}") String hdfsPath, @Value("${hbase.master}") String hbaseMaster, @Value("${hbase.zookeeper.property.clientPort}") String zookeeperClientPort,
					   @Value("${hbase.zookeeper.quorum}") String zookeeperQuorum, @Value("${hdfs.media.image.path}") String hdfsMediaImagePath,
					   @Value("${hdfs.media.video.path}") String hdfsMediaVideoPath){ // Empty Constructor with Arguments
		this.HDFS_PATH = hdfsPath;
		this.HBASE_MASTER = hbaseMaster;
		this.ZOOKEEPER_CLIENTPORT = zookeeperClientPort;
		this.ZOOKEEPER_QUORUM = zookeeperQuorum;
		this.HDFS_MEDIA_IMAGE_PATH = hdfsMediaImagePath;
		this.HDFS_MEDIA_VIDEO_PATH = hdfsMediaVideoPath;
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
	
	public static Configuration getHadoopConfig(){
		logger.info("getHadoopConfig >>>>>>>>> ");

		boolean isHadoopConfigExists = (hadoopConfig != null ? (hadoopConfig.get("fs.default.name") == HDFS_PATH) : false);
		
		if(!isHadoopConfigExists){
			hadoopConfig = new Configuration();
			hadoopConfig.set("fs.default.name", HDFS_PATH);
		}
		return hadoopConfig;
	}

	public static Configuration getHBaseConfig(){
		logger.info("HBase Master URL >>>>>>>>> "+HBASE_MASTER);

		boolean isMasterExists = (hbaseConfig != null ? (hbaseConfig.get("hbase.master") == HBASE_MASTER) : false);

		if(!isMasterExists){
			hbaseConfig = new Configuration();
			hbaseConfig.set("hbase.master", HBASE_MASTER);
			hbaseConfig.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_CLIENTPORT);
			hbaseConfig.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
		}
		return hbaseConfig;
	}
	
	public static boolean isTableExists(String tableName){
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
