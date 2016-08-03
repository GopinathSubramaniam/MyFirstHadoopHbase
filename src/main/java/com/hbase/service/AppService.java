package com.hbase.service;

import com.hbase.model.User;
import com.hbase.util.AppConstant;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@Component	
public class AppService {

	private Logger logger = LoggerFactory.getLogger(AppService.class);

	@Autowired
	private HbaseTemplate hbaseTemplate;

	@Value("${hdfs.path}")
	private String hdfsPath;
	
	public void createTables(){
		List<String> tables = new ArrayList<String>();
		tables.add("User"); //Adding table names
		for (String name : tables) {
			AppConstant.createHbaseTable(name);
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

	public void uploadMedia(MultipartFile file){
		logger.info("Upload Media >>>>>>>>>> ");
		String fileName = file.getOriginalFilename();
		try {
			FileSystem hdfs = FileSystem.get(AppConstant.getHadoopConfig());
			Path path = new Path(AppConstant.HDFS_MEDIA_IMAGE_PATH);
			Path imagePath = new Path(AppConstant.HDFS_MEDIA_IMAGE_PATH+fileName);

			if(hdfs.exists(path)){
				logger.info("Directory Already Exists");
			}else{
				hdfs.mkdirs(path);// Create directory file in HDFS if not exists
				logger.info("Directory created successfully");
			}

			if(hdfs.exists(imagePath)){
				logger.info("File Already Exists");
			}else{
				hdfs.createNewFile(imagePath);	// Upload file in HDFS if not exists
				logger.info("File Uploaded Successfully");
			}

		}catch(Exception e){
			e.printStackTrace();
		}

	}

	
}
