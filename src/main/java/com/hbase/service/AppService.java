package com.hbase.service;

import com.hbase.model.User;
import com.hbase.util.AppConstant;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedInputStream;
import java.io.InputStream;
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

	public void uploadMedia(MultipartFile file){
		logger.info("Upload Media >>>>>>>>>> ");
		String fileName = file.getOriginalFilename();
		try {
			String hdfsMediaImagePath = env.getProperty("hdfs.media.image.path");
			String hdfsMediaVideoPath = env.getProperty("hdfs.media.video.path");

			FileSystem hdfs = FileSystem.get(appConstant.getHadoopConfig());
			Path path = new Path(hdfsMediaImagePath);
			Path imagePath = new Path(hdfsMediaImagePath+fileName);

			if(hdfs.exists(path)){
				logger.info("Directory Already Exists");
				//hdfs.delete(path, true);
				//logger.info("Directory Deleted Successfully");
			}else{
				hdfs.mkdirs(path);// Create directory file in HDFS if not exists
				logger.info("Directory created successfully");
			}

			FSDataOutputStream out = hdfs.create(imagePath);
			InputStream in = new BufferedInputStream(file.getInputStream());
			byte[] b = new byte[1024];
			int numBytes = 0;
			while ((numBytes = in.read(b)) > 0) {
				out.write(b, 0, numBytes);
			}
			in.close();
			out.close();
			hdfs.close();
			System.out.println(">>>>>>>>>> Media Uploaded Successfully >>>>>>>>>>");



		}catch(Exception e){
			e.printStackTrace();
		}

	}

	
}
