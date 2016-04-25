package com.bj58.storm_realtime_analyse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

public class Save implements IRichBolt {
  private static final Log LOG = LogFactory.getLog(Save.class);
  private static final long serialVersionUID = 886149197481637896L;
  private OutputCollector collector;
  private HTable table = null;
  private Map<String, String> DBConfMap = new HashMap<String, String>();
  private String DBSystem = "";
  private Boolean ackFlag = true;
  private String topic = "";

  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    topic = (String) stormConf.get("topic");
    DBSystem = (String) stormConf.get("DBSystem");

    if (DBSystem.equals("hbase")) {
      String hbaseZookeeperQuorum = (String) stormConf.get("hbaseZookeeperQuorum");
      String hbaseTableName = (String) stormConf.get("hbaseTableName");
      String hbaseColumnfamilyName = (String) stormConf.get("hbaseColumnfamilyName");
      String hbaseColumnfamilyQualifier = (String) stormConf.get("hbaseColumnfamilyQualifier");
      DBConfMap.put("hbaseColumnfamilyName", hbaseColumnfamilyName);
      DBConfMap.put("hbaseColumnfamilyQualifier", hbaseColumnfamilyQualifier);

      final Configuration hbase_config = HBaseConfiguration.create();
      hbase_config.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);

      try {
        this.table = new HTable(hbase_config, hbaseTableName);
      } catch (IOException e) {
        LOG.error("init hbase table client error", e);
        throw new RuntimeException("init hbase table client error");
      }
    }
  }

  public void execute(Tuple input) {
    if (DBSystem.equals("hbase")) {
      ackFlag = SaveToHbase.save(input, topic, DBConfMap, table, LOG);
      if (ackFlag == true)
        collector.ack(input);
    }
  }

  public void cleanup() {
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
  }

}
