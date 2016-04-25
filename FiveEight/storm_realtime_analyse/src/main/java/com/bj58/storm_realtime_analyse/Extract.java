package com.bj58.storm_realtime_analyse;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import json.JSONObject;
import backtype.storm.tuple.Fields;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Extract implements IRichBolt {

  private static final Log LOG = LogFactory.getLog(Extract.class);
  private static final long serialVersionUID = 886149197481637894L;
  private OutputCollector collector;
  private Map<Integer, List<String>> tasksConfMap = null;
  private String topic = "";
  private Map<Integer, List<String>> tempTasksConfMap = null;

  private String mysqlDB = "";
  private String mysqlTable = "";
  private String mysqlUser = "";
  private String mysqlPWD = "";

  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    mysqlDB = (String) stormConf.get("mysqlDB");
    mysqlTable = (String) stormConf.get("mysqlTable");
    mysqlUser = (String) stormConf.get("mysqlUser");
    mysqlPWD = (String) stormConf.get("mysqlPWD");
    this.collector = collector;
    topic = (String) stormConf.get("topic");

    new Thread(new Runnable() {
      public void run() {
        while (true) {
          try {
            tempTasksConfMap = ParseConfFromMysql.parseConfFromMysql(mysqlDB, mysqlTable, mysqlUser, mysqlPWD, topic);
            tasksConfMap = tempTasksConfMap;
          } catch (ClassNotFoundException e) {
            if (tasksConfMap == null)
              throw new RuntimeException("class" + " com.mysql.jdbc.Driver" + " not found");
            else
              LOG.error("class" + " com.mysql.jdbc.Driver" + " not found", e);
          } catch (SQLException e) {
            if (tasksConfMap == null)
              throw new RuntimeException(e.getCause());
            else
              LOG.error("import java.sql.SQLException", new Throwable(e.getCause()));
          }

          try {
            Thread.sleep(1000 * 120);
          } catch (InterruptedException e) {
            LOG.error("InterruptedException: update tasksConfMap form mysql watcher sleep interrupted",
                new InterruptedException(
                    "InterruptedException: update tasksConfMap form mysql watcher sleep interrupted"));
          }
        }
      }
    }).start();
  }

  public void execute(Tuple input) {
    if (tasksConfMap != null) {
      String line = input.getString(0);
      Set<Map.Entry<Integer, List<String>>> tasksConfMapSet = tasksConfMap.entrySet();
      for (Map.Entry<Integer, List<String>> e : tasksConfMapSet) {
        try {
          String id = e.getKey() + "";
          List<String> taskConfArray = e.getValue();
          String dimensionGroupJOStr = taskConfArray.get(0);
          String measureJOStr = taskConfArray.get(1);
          String timeInterval = taskConfArray.get(2);
          String[] separator = {""};
          Boolean dimensionsSatisfied = true;
          String measureField = "";
          String method = "";

          dimensionsSatisfied = DimensionVerification.dimensionVerification(id, dimensionGroupJOStr, line, separator);
          if (dimensionsSatisfied == false)
            continue;

          JSONObject measureJO = new JSONObject(measureJOStr);
          if (measureJO.get("r") == null) {
            String[] fields = line.split(separator[0]);
            int index = measureJO.getInt("i");
            measureField = fields[index];    
            method = measureJO.getString("f");
          }

          else if (measureJO.get("i") == null) {
            String regex = measureJO.getString("r");
            ArrayList<String> strList = new ArrayList<String>();
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(line);

            while (matcher.find()) {
              int start = matcher.start();
              int end = matcher.end();
              String field = line.substring(start, end);
              strList.add(field);
            }

            if (strList.size() != 1) {
              throw new RuntimeException(
                  "multiple or no matching regex result, task id:" + id + " patternStr:" + regex);
            } else
              measureField = strList.get(0);
            method = measureJO.getString("f");
          }

          else {
            String[] fields = line.split(separator[0]);
            int index = measureJO.getInt("i");
            String intermediateField = fields[index];

            String regex = measureJO.getString("r");
            ArrayList<String> strList = new ArrayList<String>();
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(intermediateField);
            while (matcher.find()) {
              int start = matcher.start();
              int end = matcher.end();
              String field = line.substring(start, end);
              strList.add(field);
            }
            if (strList.size() != 1) {
              throw new RuntimeException(
                  "multiple or no matching regex result, task id:" + id + " patternStr:" + regex);
            } else
              measureField = strList.get(0);
            method = measureJO.getString("f");
          }
          collector.emit(input, new Values(id, measureField, method, timeInterval));
        } catch (Exception exp) {
          LOG.error("task extract error", exp);
        }
      }
    }
    if (tasksConfMap != null)
      collector.ack(input);
    else
      collector.fail(input);
  }

  public void cleanup() {
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("id", "measureField", "method", "timeInterval"));
  }

}
