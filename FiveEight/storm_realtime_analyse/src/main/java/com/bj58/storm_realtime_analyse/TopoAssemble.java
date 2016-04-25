package com.bj58.storm_realtime_analyse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TopoAssemble {
  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, IOException {
    String argsFilePath = args[0];
    if (argsFilePath == null || argsFilePath.trim().equals("")) {
        throw new IllegalArgumentException(" arguments file null");
    }
    
    Map<String, String> argsMap = new HashMap<String, String>();
    File file = new File(argsFilePath);
    BufferedReader br = null;
    br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
    String line = null;
    int lineNum = 0;
    try {
      while ((line = br.readLine()) != null) {
        String parts[] = line.split(" ");
        if (parts == null || parts.length != 2) {
          throw new IllegalArgumentException("argument from file wrong, lineNum:" + lineNum);
        }
        argsMap.put(parts[0], parts[1]);
        lineNum++;
      }
    } finally {
      br.close();
    }
    
    String brokerZkStr = argsMap.get("brokerZkStr");
    String brokerZkPath = argsMap.get("brokerZkPath");
    String topic = argsMap.get("topic");
    String zkRoot = argsMap.get("zkRoot");
    String id = argsMap.get("id");
    
//    BrokerHosts brokerHosts = new ZkHosts(brokerZkStr, brokerZkPath);
    BrokerHosts brokerHosts = new ZkHosts(brokerZkStr);
    SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
    spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
    spoutConf.forceFromStart = false;
    spoutConf.zkServers = Arrays.asList(new String[] {"10.9.14.26"});
    spoutConf.zkPort = 2181;

    int kafkaReaderExecutorNum = Integer.parseInt(argsMap.get("kafkaReaderExecutorNum"));
    int extractExecutorNum = Integer.parseInt(argsMap.get("extractExecutorNum"));
    int logicExecutorNum = Integer.parseInt(argsMap.get("logicExecutorNum"));
    int saveExecutorNum = Integer.parseInt(argsMap.get("saveExecutorNum"));
    
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("kafkaReader", new KafkaSpout(spoutConf), kafkaReaderExecutorNum);
    builder.setBolt("extract", new Extract(), extractExecutorNum).shuffleGrouping("kafkaReader");
    builder.setBolt("logic", new Logic(), logicExecutorNum).fieldsGrouping("extract",
        new Fields("id"));
    builder.setBolt("save", new Save(), saveExecutorNum).fieldsGrouping("logic",
        new Fields("id"));
    
    Config stormConf = new Config();
    
    int workerNum = Integer.parseInt(argsMap.get("workerNum"));
    stormConf.setNumWorkers(workerNum);
    
    stormConf.put("topic", topic);
    
    String mysqlDB = argsMap.get("mysqlDB");
    String mysqlTable = argsMap.get("mysqlTable");
    String mysqlUser = argsMap.get("mysqlUser");
    String mysqlPWD = argsMap.get("mysqlPWD");
    stormConf.put("mysqlDB", mysqlDB);
    stormConf.put("mysqlTable", mysqlTable);
    stormConf.put("mysqlUser", mysqlUser);
    stormConf.put("mysqlPWD", mysqlPWD);
   
    String hbaseZookeeperQuorum = argsMap.get("hbaseZookeeperQuorum");
    String hbaseTableName = argsMap.get("hbaseTableName");
    String hbaseColumnfamilyName = argsMap.get("hbaseColumnfamilyName");
    String hbaseColumnfamilyQualifier = argsMap.get("hbaseColumnfamilyQualifier");
    stormConf.put("hbaseZookeeperQuorum", hbaseZookeeperQuorum);
    stormConf.put("hbaseTableName", hbaseTableName);
    stormConf.put("hbaseColumnfamilyName", hbaseColumnfamilyName);
    stormConf.put("hbaseColumnfamilyQualifier", hbaseColumnfamilyQualifier);
    
    String DBSystem = argsMap.get("DBSystem");
    stormConf.put("DBSystem", DBSystem);

    String topoName = argsMap.get("topoName");
    StormSubmitter.submitTopologyWithProgressBar(topoName, stormConf, builder.createTopology());
  }

}
