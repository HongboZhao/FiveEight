package com.bj58.storm_realtime_analyse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import json.*;

public class ParseConfFromMysql {

  public static Map<Integer, List<String>> parseConfFromMysql(String mysqlDB, String mysqlTable, String mysqlUser,
      String mysqlPWD, String topic) throws SQLException, ClassNotFoundException{
    String driverName = "com.mysql.jdbc.Driver";
    String sql = "";
    ResultSet res = null;
    Class.forName(driverName);
    Connection conn = DriverManager.getConnection("jdbc:mysql://" + mysqlDB, mysqlUser, mysqlPWD);
    Statement stmt = null;
    stmt = conn.createStatement();
    sql = "select * from " + mysqlTable + " where topic=" + "'" + topic + "'";
    res = stmt.executeQuery(sql);
    Map<Integer, List<String>> tasksConfMap = createTaskConfMap(res);
    return tasksConfMap;
  }
  
  public static Map<Integer, List<String>> createTaskConfMap(ResultSet res) throws SQLException {
    Map<Integer, List<String>> tasksConfMap = new HashMap<Integer, List<String>>();

    while (res.next()) {
      int id = res.getInt("id");

      String dimensionGroupJOStr = res.getString("dimensions");
      String measure = res.getString("measure");
      String interval = res.getString("interval");
      List<String> taskMapValue = new ArrayList<String>();
      taskMapValue.add(0, dimensionGroupJOStr);
      taskMapValue.add(1, measure);
      taskMapValue.add(2, interval);

      tasksConfMap.put(id, taskMapValue);
    }
    return tasksConfMap;
  }

}
