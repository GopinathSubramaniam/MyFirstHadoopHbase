package com.hbase.controller;

import com.hbase.model.Status;
import com.hbase.service.FileUploadService;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

/**
 * Created by Gopi on 04-08-2016.
 */

@RestController
@RequestMapping("/upload")
public class FileUploadAdapter {


    @Autowired
    private FileUploadService uploadService;

    @Autowired
    private Environment env;

    @RequestMapping(value="/image", method= RequestMethod.POST)
    public Status uploadImage(@RequestParam("file") MultipartFile file, @RequestParam("user") String data)  throws Exception{
        JSONObject userObj = new JSONObject(data);

        String path = env.getProperty("hdfs.media.image.path");
        Status status = null;
        if(file != null && path!=null) {
            status = uploadService.uploadMedia(file, path);
        }else{
            status.setStatus("FAIL");
            status.setStatusCode(500);
            status.setStatusMsg("Both FILE & PATH must contain some value");
        }
        return status;
    }

}
