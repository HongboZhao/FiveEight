package com.bj58.storm_realtime_analyse;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.commons.logging.Log;

import backtype.storm.tuple.Tuple;

public class SaveToHbase {

  public static Boolean save(Tuple input, String topic, Map<String, String> DBConf, HTable table, Log LOG) {
    String hbaseColumnfamilyName = DBConf.get("hbaseColumnfamilyName");
    String hbaseColumnfamilyQualifier = DBConf.get("hbaseColumnfamilyQualifier");
    Boolean ackFlag = true;

    String id = input.getStringByField("id");
    String method = input.getStringByField("method");
    String time = input.getStringByField("time");
    Number value = null;
    if (method.equals("count"))
      value = input.getLongByField("value");
    if (method.equals("sum") || method.equals("min") || method.equals("max") || method.equals("average"))
      value = input.getDoubleByField("value");

    String rowkeyStr = id + "-" + time;
    System.out.println(rowkeyStr + "\t" + value);

    Put put = new Put(Bytes.toBytes(rowkeyStr));
    put.add(Bytes.toBytes(hbaseColumnfamilyName), Bytes.toBytes(hbaseColumnfamilyQualifier), Bytes.toBytes(value + ""));
    try {
      table.put(put);
    } catch (IOException e) {
      ackFlag = false;
      LOG.error("IOException: put data to hbase error, sleep 1s", new IOException("put data to hbase error"));
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        LOG.error("put data to hbase error, sleep interrupted",
            new InterruptedException("put data to hbase error, sleep interrupted"));
      }
    }
    return ackFlag;
  }
}
