package com.bj58.storm_realtime_analyse;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import backtype.storm.tuple.Tuple;

public class TaskValue {
  private int timePassed = 0;
  private Object value = null;
  private List<Tuple> anchors = new CopyOnWriteArrayList<Tuple>();
  
  public TaskValue() {
  }
  
  public TaskValue(int timePassed, Object value, List<Tuple> anchors) {
    this.timePassed = timePassed;
    this.value = value;
    this.anchors = anchors;
  }
  
  public void setTimePassed(int timePassed) {
    this.timePassed = timePassed;
  }
  
  public int getTimePassed() {
    return timePassed;
  }
  
  public synchronized void setValue(Object value) {
    this.value = value;
  }
  
  public synchronized Object getValue() {
    return value;
  }
  
  public void setAnchors(List<Tuple> anchors) {
    this.anchors = anchors;
  }
  
  public List<Tuple> getAnchors() {
    return anchors;
  }
}
