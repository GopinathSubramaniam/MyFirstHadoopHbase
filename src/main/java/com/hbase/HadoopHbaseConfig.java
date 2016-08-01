package com.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.config.annotation.EnableHadoop;
import org.springframework.data.hadoop.config.annotation.SpringHadoopConfigurerAdapter;
import org.springframework.data.hadoop.config.annotation.builders.HadoopConfigConfigurer;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

@Configuration
@EnableHadoop
public class HadoopHbaseConfig extends SpringHadoopConfigurerAdapter {

	private static String HDFS_PATH = "hdfs://192.168.0.112:9000/";
	private static String HADOOP_HOME_DIR = "/home/hadoop/hadoop/";
	
	private static String HADOOP_HOME_DIR_FOR_WINDOWS = "C:\\hadoop_win_files";

	@Override
	public void configure(HadoopConfigConfigurer config) throws Exception {
		config.fileSystemUri(HDFS_PATH).withProperties().property("hadoop.home.dir", HADOOP_HOME_DIR);
		System.setProperty("hadoop.home.dir", HADOOP_HOME_DIR_FOR_WINDOWS);
	}
	
	@Bean
	public org.apache.hadoop.conf.Configuration hBaseConfiguration(){
		System.out.println(">>>>>>>>>>>>>>>>>> HBASE CONFIG >>>>>>>>>>>>>>>>>>");
		org.apache.hadoop.conf.Configuration hbaseConfig = new HBaseConfiguration().create();
		hbaseConfig.set("hbase.master","192.168.0.112:60000");
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
		hbaseConfig.set("hbase.zookeeper.quorum", "192.168.0.112");
		
		return hbaseConfig;
	}
	
	@Bean
	public HbaseTemplate hbaseTemplate(){
		HbaseTemplate hbaseTemplate = new HbaseTemplate();
		hbaseTemplate.setConfiguration(hBaseConfiguration());
		System.out.println(">>>>>>>>>>>>>>>>>> HBASE TEMPLATE >>>>>>>>>>>>>>>>>>");
		return hbaseTemplate;
	}

}
