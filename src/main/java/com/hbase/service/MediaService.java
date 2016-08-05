package com.hbase.service;

import com.hbase.model.Status;
import com.hbase.util.AppConstant;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by Gopi on 04-08-2016.
 */

@Component
public class MediaService {

    private Logger logger = LoggerFactory.getLogger(MediaService.class);

    @Autowired
    private AppConstant appConstant;

    /*@Autowired
    private Environment env;*/

    public Status uploadMedia(MultipartFile file, String path){
        logger.info("Upload Media Called");
        Status status = uploadMediaInHDFS(file, path);
        return status;
    }

    public  void getMedia(HttpServletResponse res, String fileName, String path){
        FileSystem hdfs = null;
        FSDataInputStream fsDataInputStream = null;
        OutputStream os = null;
        try {
            hdfs = FileSystem.get(appConstant.getHadoopConfig());
            Path fileUrl = new Path(path+fileName);
            fsDataInputStream = hdfs.open(fileUrl);

            res.setContentType("application/octet-stream");
            res.setHeader("Content-Disposition", "attachment; filename=\"" + fileName + "\"");

            os = res.getOutputStream();
            byte[] buffer = new byte[1024];
            int len;
            while ((len = fsDataInputStream.read(buffer)) != -1) {
                os.write(buffer, 0, len);
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try {
                os.flush();
                os.close();
                fsDataInputStream.close();

            }catch (Exception fe){
                fe.printStackTrace();
            }
        }
    }

    private Status uploadMediaInHDFS(MultipartFile file, String path){
        Status status = new Status();
        String fileName = file.getOriginalFilename();
        try {
            FileSystem hdfs = FileSystem.get(appConstant.getHadoopConfig()); // Getting Hadoop Configuration
            Path hdfsMediaPath = new Path(path);// Media Location Ex: /usr/media/
            Path hdfsFilePath = new Path(path+fileName); //Media Location with file name Ex: /usr/media/IMG_01.jpg

            if(hdfs.exists(hdfsMediaPath)){
                logger.info("Directory Already Exists");
                //hdfs.delete(path, true);
                //logger.info("Directory Deleted Successfully");
            }else{
                hdfs.mkdirs(hdfsMediaPath);// Create directory file in HDFS if not exists
                logger.info("Directory created successfully");
            }
            // Uploading file in HDFS START
            FSDataOutputStream out = hdfs.create(hdfsFilePath); // Creating path in HDFS

            InputStream in = new BufferedInputStream(file.getInputStream());
            byte[] b = new byte[1024];
            int numBytes = 0;
            while ((numBytes = in.read(b)) > 0) {
                out.write(b, 0, numBytes);
            }
            in.close();
            out.close();
            hdfs.close();
            // Uploading file in HDFS END
            logger.info("Media Uploaded Successfully");
            status.setStatus("OK");
            status.setStatusCode(200);
            status.setStatusMsg("Media " + fileName+" Uploaded Successfully");
        }catch (Exception e){
            e.printStackTrace();
            status.setStatus("FAIL");
            status.setStatusCode(500);
            status.setStatusMsg(e.getMessage());
        }
        return status;
    }


}
