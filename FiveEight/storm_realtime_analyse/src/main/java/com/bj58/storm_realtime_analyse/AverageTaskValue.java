package com.bj58.storm_realtime_analyse;

public class AverageTaskValue {
  private Double totalValue = 0.0;
  private int recordNum = 0;
  
  public AverageTaskValue() {
  }

  public AverageTaskValue(Double totalValue, int recordNum) {
    this.totalValue =totalValue;
    this.recordNum = recordNum;
  }
  
  
  public void setTotalValue(Double totalValue) {
    this.totalValue = totalValue;
  }
  
  public Double getTotalValue() {
    return totalValue;
  }
  
  public void setRecordNum(int recordNum) {
    this.recordNum = recordNum;
  }
  
  public int getRecordNum() {
    return recordNum;
  }
  
  public String toString() {
    Double average = totalValue / recordNum;
    return average.toString();
  }
}
