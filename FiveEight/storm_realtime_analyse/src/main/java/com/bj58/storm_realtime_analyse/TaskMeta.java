package com.bj58.storm_realtime_analyse;

class TaskMeta {
  private String id = "";
  private String method = "";
  private int timeInterval = 1;
  private Boolean isTopN = false;

  public TaskMeta() {
  }

  public TaskMeta(String dimensionalityGroup, String measure, int timeInterval, Boolean isTopN) {
    this.id = dimensionalityGroup;
    this.method = measure;
    this.timeInterval = timeInterval;
    this.isTopN = isTopN;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public void setMethod(String method) {
    this.method = method;
  }

  public String getMethod() {
    return method;
  }

  public void setTimeInterval(int timeInterval) {
    this.timeInterval = timeInterval;
  }

  public int getTimeInterval() {
    return timeInterval;
  }
  
  public void setIsTopN(Boolean isTopN) {
    this.isTopN = isTopN;
  }
  
  public Boolean getIsTopN() {
    return isTopN;
  }
}