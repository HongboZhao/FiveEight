package com.bj58.storm_realtime_analyse;

import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Logic implements IRichBolt {

  private static final Log LOG = LogFactory.getLog(Logic.class);
  private static final long serialVersionUID = 886149197481637894L;
  private OutputCollector collector = null;
  private String topic = "";
  private String time = "";
  private ConcurrentHashMap<String, TaskMeta> taskMetaMap = new ConcurrentHashMap<String, TaskMeta>();
  private ConcurrentHashMap<String, TaskValue> taskValueMap = new ConcurrentHashMap<String, TaskValue>();
  private Lock countLock = new ReentrantLock();
  private Lock sumLock = new ReentrantLock();
  private Lock minLock = new ReentrantLock();
  private Lock maxLock = new ReentrantLock();
  private Lock averageLock = new ReentrantLock();

  private String monthsStr = "";
  private String daysStr = "";
  private String hoursStr = "";
  private String minutesStr = "";
  private String secondsStr = "";

  private String mysqlDB = "";
  private String mysqlTable = "";
  private String mysqlUser = "";
  private String mysqlPWD = "";

  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    topic = (String) stormConf.get("topic");

    mysqlDB = (String) stormConf.get("mysqlDB");
    mysqlTable = (String) stormConf.get("mysqlTable");
    mysqlUser = (String) stormConf.get("mysqlUser");
    mysqlPWD = (String) stormConf.get("mysqlPWD");

    new Thread(new Runnable() {
      private Long emitStartTime = new Long(0);
      private Long emitEndTime = new Long(0);

      public void run() {
        while (true) {
          try {
            if (emitEndTime - emitStartTime <= 1000)
              Thread.sleep(1000 - emitEndTime + emitStartTime);
            else {
              Thread.sleep(1000 * ((emitEndTime - emitStartTime) / 1000 + 1) - emitEndTime + emitStartTime);
            }
          } catch (InterruptedException e) {
            LOG.error("emit watcher sleep interruptedException",
                new InterruptedException("emit watcher sleep interruptedException"));
          }
          emitStartTime = System.currentTimeMillis();
          watcherEmit();
          emitEndTime = System.currentTimeMillis();
        }
      }
    }).start();

    new Thread(new Runnable() {
      public void run() {
        while (true) {
          try {
            Thread.sleep(1000 * 120);
          } catch (InterruptedException e) {
            LOG.error("emit watcher sleep interruptedException",
                new InterruptedException("emit watcher sleep interruptedException"));
          }
          updateTaskMap();
        }
      }
    }).start();
  }

  public void watcherEmit() {
    Calendar calendar = Calendar.getInstance();
    int years = calendar.get(Calendar.YEAR);
    int months = calendar.get(Calendar.MONTH) + 1;
    if (months < 10)
      monthsStr = "0" + months;
    else
      monthsStr = "" + months;
    int days = calendar.get(Calendar.DAY_OF_MONTH);
    if (days < 10)
      daysStr = "0" + days;
    else
      daysStr = "" + days;
    int hours = calendar.get(Calendar.HOUR_OF_DAY);
    if (hours < 10)
      hoursStr = "0" + hours;
    else
      hoursStr = "" + hours;
    int minutes = calendar.get(Calendar.MINUTE);
    if (minutes < 10)
      minutesStr = "0" + minutes;
    else
      minutesStr = "" + minutes;
    int seconds = calendar.get(Calendar.SECOND);
    if (seconds < 10)
      secondsStr = "0" + seconds;
    else
      secondsStr = "" + seconds;
    time = String.format("%s.%s.%s:%s:%s:%s", years, monthsStr, daysStr, hoursStr, minutesStr, secondsStr);

    Set<Map.Entry<String, TaskMeta>> taskMetaEntrySet = taskMetaMap.entrySet();
    for (Map.Entry<String, TaskMeta> e : taskMetaEntrySet) {
      String taskMetaMapKey = e.getKey();
      TaskMeta taskMeta = e.getValue();
      TaskValue taskValue = taskValueMap.get(taskMetaMapKey);
      if (taskValue == null)
        continue;
      taskValue.setTimePassed(taskValue.getTimePassed() + 1);
      if ((taskValue.getTimePassed()) >= taskMeta.getTimeInterval()) {
        String id = taskMeta.getId();
        String method = taskMeta.getMethod();
        Object value = taskValue.getValue();
        List<Tuple> anchors = taskValue.getAnchors();
        collector.emit(anchors, new Values(id, method, time, value));

        taskValue.getAnchors().clear();
        taskValue.setTimePassed(0);
        if (method.equals("count")) {
          countLock.lock();
          try {
            taskValue.setValue(new Long(0));
            taskValue.getAnchors().clear();
          } finally {
            countLock.unlock();
          }
        }
        if (method.equals("sum")) {
          sumLock.lock();
          try {
            taskValue.setValue(new Double(0.0));
            taskValue.getAnchors().clear();
          } finally {
            sumLock.unlock();
          }
        }
        if (method.equals("max")) {
          maxLock.lock();
          try {
            taskValue.setValue(new Double(Double.MIN_VALUE));
            taskValue.getAnchors().clear();
          } finally {
            maxLock.unlock();
          }
        }
        if (method.equals("min")) {
          minLock.lock();
          try {
            taskValue.setValue(new Double(Double.MAX_VALUE));
            taskValue.getAnchors().clear();
          } finally {
            minLock.unlock();
          }
        }
        if (method.equals("average")) {
          averageLock.lock();
          try {
            taskValue.setValue(new AverageTaskValue(0.0, 0));
            taskValue.getAnchors().clear();
          } finally {
            averageLock.unlock();
          }
        }
      } 
    }
  }

  public void updateTaskMap() {
    Map<Integer, List<String>> tasksMap = null;
    try {
      tasksMap = ParseConfFromMysql.parseConfFromMysql(mysqlDB, mysqlTable, mysqlUser, mysqlPWD, topic);
    } catch (Exception e) {
      LOG.error("get taskKeyArray form mysql error", new Throwable(e));
    }

    if (tasksMap != null) {
      Set<Map.Entry<String, TaskMeta>> taskMetaEntrySet = taskMetaMap.entrySet();
      for (Map.Entry<String, TaskMeta> e : taskMetaEntrySet) {
        String taskMetaMapKey = e.getKey();
        if (!(tasksMap.containsKey(taskMetaMapKey))) {
          taskMetaMap.remove(taskMetaMapKey);
          taskValueMap.remove(taskMetaMapKey);
        }
      }
    }
  }

  public void execute(Tuple input) {
    String id = input.getStringByField("id");
    String measureField = input.getStringByField("measureField");
    String method = input.getStringByField("method");
    String timeInterval = input.getStringByField("timeInterval");
    doWork(input, id, measureField, method, timeInterval);
    collector.ack(input);
  }

  public void doWork(Tuple input, String id, String measureField, String method, String timeInterval) {
    String key = id;

    if (method.equals("count")) {
      countLock.lock();
      try {
        if (!(taskMetaMap.containsKey(key))) {
          taskMetaMap.put(key, new TaskMeta(id, method, Integer.parseInt(timeInterval), false));
          taskValueMap.put(key, new TaskValue(0, new Long(1), new CopyOnWriteArrayList<Tuple>()));
        } else
          taskValueMap.get(key).setValue((Long) (taskValueMap.get(key).getValue()) + 1);
        taskValueMap.get(key).getAnchors().add(input);
      } finally {
        countLock.unlock();
      }
    }

    if (method.equals("sum")) {
      sumLock.lock();
      try {
        if (!(taskMetaMap.containsKey(key))) {
          taskMetaMap.put(key, new TaskMeta(id, method, Integer.parseInt(timeInterval), false));
          taskValueMap.put(key, new TaskValue(0, Double.parseDouble(measureField), new CopyOnWriteArrayList<Tuple>()));
        } else
          taskValueMap.get(key)
              .setValue(((Double) (taskValueMap.get(key).getValue())) + Double.parseDouble(measureField));
        taskValueMap.get(key).getAnchors().add(input);
      } finally {
        sumLock.unlock();
      }
    }

    if (method.equals("min")) {
      minLock.lock();
      try {
        Double newValue = Double.parseDouble(measureField);
        if (!(taskMetaMap.containsKey(key))) {
          taskMetaMap.put(key, new TaskMeta(id, method, Integer.parseInt(timeInterval), false));
          taskValueMap.put(key, new TaskValue(0, newValue, new CopyOnWriteArrayList<Tuple>()));
        } else if (newValue < (Double) (taskValueMap.get(key).getValue()))
          taskValueMap.get(key).setValue(newValue);
        taskValueMap.get(key).getAnchors().add(input);
      } finally {
        minLock.unlock();
      }
    }

    if (method.equals("max")) {
      maxLock.lock();
      try {
        Double newValue = Double.parseDouble(measureField);
        if (!(taskMetaMap.containsKey(key))) {
          taskMetaMap.put(key, new TaskMeta(id, method, Integer.parseInt(timeInterval), false));
          taskValueMap.put(key, new TaskValue(0, newValue, new CopyOnWriteArrayList<Tuple>()));
        } else if (newValue > (Double) (taskValueMap.get(key).getValue()))
          taskValueMap.get(key).setValue(newValue);
        taskValueMap.get(key).getAnchors().add(input);
      } finally {
        maxLock.unlock();
      }
    }

    if (method.equals("average")) {
      averageLock.lock();
      try {
        Double newValue = Double.parseDouble(measureField);
        if (!(taskMetaMap.containsKey(key))) {
          taskMetaMap.put(key, new TaskMeta(id, method, Integer.parseInt(timeInterval), false));
          taskValueMap.put(key, new TaskValue(0, new AverageTaskValue(newValue, 0), new CopyOnWriteArrayList<Tuple>()));
        } else {
          AverageTaskValue oldAverageTaskValue = (AverageTaskValue) (taskValueMap.get(key).getValue());
          Double oldTotalValue = oldAverageTaskValue.getTotalValue();
          int oldRecordNum = oldAverageTaskValue.getRecordNum();
          taskValueMap.get(key).setValue(new AverageTaskValue(oldTotalValue + newValue, oldRecordNum + 1));
        }
        taskValueMap.get(key).getAnchors().add(input);
      } finally {
        averageLock.unlock();
      }
    }

    if (taskMetaMap.get(key).getTimeInterval() != Integer.parseInt(timeInterval)) {
      taskMetaMap.get(key).setTimeInterval(Integer.parseInt(timeInterval));
    }

  }

  public void cleanup() {
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("id", "method", "time", "value"));
  }

}