//package com.mapr.test;
import java.io.IOException;
import java.util.*;
import java.lang.Long;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class MaprDbBinCURD {
	//method to create table
	public static void createTable(Configuration conf, String tableName) throws IOException {
		  System.out.println("In CreateTable");

	    HBaseAdmin admin = new HBaseAdmin(conf);
	    System.out.println("In CreateTable HBaseAdmin initilized");
	    HTableDescriptor des = new HTableDescriptor(tableName.getBytes());
	    System.out.println("In CreateTable HTableDescriptor");
	    for (int i = 0; i < 2; i++) {
	      des.addFamily(new HColumnDescriptor("f" + i));
	    }
		try {
			System.out.println("In CreateTable creating table");
		  	admin.createTable(des);
		  	System.out.println("In CreateTable after create table");
		} catch (TableExistsException te) {
			System.out.println("TableExistException"+ te);
		}
	 }
	 public static void scanTable(Configuration conf, String tableName) throws IOException {
	 		System.out.println("Scan is invoked");
	 		try {
	 			byte[] M7Table = Bytes.toBytes(tableName);
	 			HTableInterface table1 = new HTable  (conf, M7Table);
	 			Scan scanobject = new Scan();
	 			ResultScanner rs = table1.getScanner(scanobject);
	 			try{    for (Result result: rs) {
	 							 for(KeyValue kv : result.raw()){
	 							System.out.print(new String(kv.getRow()) + " ");
	 								System.out.print(new String(kv.getFamily()) + ":");
	 									System.out.print(new String(kv.getQualifier()) + " ");
	 									 System.out.print(kv.getTimestamp() + " ");
	 									System.out.println(new String(kv.getValue()));}
	 					}
	 			   }finally {
					   rs.close();
				   }
	 			 table1.close();

	 		} catch (IOException e) {
	 		   e.printStackTrace();
	 		}
	} //end of scan

	public static void getTable(Configuration conf, String tableName) throws IOException {
			System.out.println("get is invoked");
			Result result = null;
			try {
				byte[] M7Table = Bytes.toBytes(tableName);
				HTableInterface table1 = new HTable  (conf, M7Table);
				String key ="1";
				Get q= new Get(Bytes.toBytes(key));
				q.setMaxVersions(10);
	 			result= table1.get(q);
				try{
					for(KeyValue kv : result.raw()){
							System.out.print(new String(kv.getRow()) + " ");
							System.out.print(new String(kv.getFamily()) + ":");
							System.out.print(new String(kv.getQualifier()) + " ");
							System.out.print(kv.getTimestamp() + " ");
							System.out.println(new String(kv.getValue()));
					}
				   }finally {
					   //result.close();
				   }
				 table1.close();

			} catch (IOException e) {
			   e.printStackTrace();}

	} //end of get

	public static void deleteTable(Configuration conf, String tableName) throws IOException {
	   		System.out.println("delete is invoked");
		HTableInterface table1 = null;
	        try {
				byte[] M7Table = Bytes.toBytes(tableName);
				table1 = new HTable  (conf, M7Table);
				String key ="1";
	            Delete d = new Delete(Bytes.toBytes(key));
	            table1.delete(d);
	        } finally {
	            table1.close();
	        }
    }
    public static void putTable(Configuration conf, String tableName) throws IOException {
		  System.out.println("In put method");
	    HTable inTable = new HTable(conf, tableName);
	    inTable.setAutoFlush(false);
	    String row;
	    String family;
	    String column;
	    String chksumColumn;
	    long uniqueSeed = System.currentTimeMillis();

	    Put info = new Put("1".getBytes());
	    info.add("f0".getBytes(), "c0".getBytes(), Bytes.toBytes("Test data"));
	    info.add("f1".getBytes(), "c1".getBytes(), Bytes.toBytes("Test data 2"));
	    inTable.put(info);

	    inTable.flushCommits();
	  }
	  public static void dropTable(Configuration conf, String tableName) throws IOException {
	  		  System.out.println("In dropTable");

	  	    HBaseAdmin admin = new HBaseAdmin(conf);
	  	    System.out.println("In drop HBaseAdmin initilized");
	  		try {
	  			System.out.println("In dropTable disable table");
	  		  	admin.disableTable(tableName);
	  		  	System.out.println("In dropTable drop table");
	  		  	admin.deleteTable(tableName);
	  		} catch (TableExistsException te) {
	  			System.out.println("TableExistException"+ te);
	  		}
	 }
	public static void main(String[] args){
			System.out.println("M7ReadTomcat is invoked");
		try {
				Configuration conf = HBaseConfiguration.create();
				String tableName = "/maprdbtable";
				createTable(conf,tableName);
				putTable(conf,tableName);
				scanTable(conf,tableName);
				getTable(conf,tableName);
				deleteTable(conf,tableName);
				dropTable(conf,tableName);

			} catch (IOException e) {
			 	e.printStackTrace();
			 }


	}
}

