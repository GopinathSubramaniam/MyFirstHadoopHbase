package com.hbase.model;

import lombok.Getter;
import lombok.Setter;
import org.springframework.web.multipart.MultipartFile;

/**
 * Created by Gopi on 03-08-2016.
 */

@Setter
@Getter
public class UploadFile {

    private MultipartFile file;

}
