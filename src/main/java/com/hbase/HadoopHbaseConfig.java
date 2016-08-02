package com.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.hadoop.config.annotation.EnableHadoop;
import org.springframework.data.hadoop.config.annotation.SpringHadoopConfigurerAdapter;
import org.springframework.data.hadoop.config.annotation.builders.HadoopConfigConfigurer;
import org.springframework.data.hadoop.hbase.HbaseTemplate;



@Configuration
@PropertySource("classpath:application.properties")
@EnableHadoop
public class HadoopHbaseConfig extends SpringHadoopConfigurerAdapter {

	private Logger logger = LoggerFactory.getLogger(HadoopHbaseConfig.class);
	private static String HADOOP_HOME_DIR_FOR_WINDOWS = "C:\\hadoop_win_files"; // Only for windows OS

	@Value("${hdfs.path}")
	private String hdfsPath;
	
	@Value("${hadoop.home.dir}")
	private String hadoopHomeDir;
	
	@Value("${hbase.master}")
	private String hbaseMaster;
	
	@Value("${hbase.zookeeper.property.clientPort}")
	private String zookeeperClientPort;
	
	@Value("${hbase.zookeeper.quorum}")
	private String zookeeperQuorum;

	@Override
	public void configure(HadoopConfigConfigurer config) throws Exception {
		config.fileSystemUri(hdfsPath).withProperties().property("hadoop.home.dir", hadoopHomeDir);
		System.setProperty("hadoop.home.dir", HADOOP_HOME_DIR_FOR_WINDOWS);
		logger.info(">>>>>>>>> SETTING HADOOP HDFS PATH >>>>>>>>>");
	}
	
	@Bean
	public org.apache.hadoop.conf.Configuration hBaseConfiguration(){
		logger.info(">>>>>>>>> HBASE CONFIG CREATION >>>>>>>>>");

		org.apache.hadoop.conf.Configuration hbaseConfig = new HBaseConfiguration().create();
		hbaseConfig.set("hbase.master", hbaseMaster);
		hbaseConfig.set("hbase.zookeeper.property.clientPort", zookeeperClientPort);
		hbaseConfig.set("hbase.zookeeper.quorum", zookeeperQuorum);
		
		return hbaseConfig;
	}
	
	@Bean
	public HbaseTemplate hbaseTemplate(){
		HbaseTemplate hbaseTemplate = new HbaseTemplate();
		hbaseTemplate.setConfiguration(hBaseConfiguration());
		logger.info(">>>>>>>>> HBASE TEMPLATE BEAN CREATION >>>>>>>>>");
		return hbaseTemplate;
	}

}
