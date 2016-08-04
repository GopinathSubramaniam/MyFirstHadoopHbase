package com.hbase.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Created by Gopi on 04-08-2016.
 */

@Setter
@Getter
@ToString
public class Status {

    private String status;
    private Integer statusCode;
    private String statusMsg;
    private Object obj;

}
