package com.hbase.util;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
public class TableSchema {
	
	private String columnName;
	private String columnType;

}
