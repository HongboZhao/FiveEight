package com.bj58.storm_realtime_analyse;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import json.*;

public class DimensionVerification {

  public static Boolean dimensionVerification(String id, String dimensionGroupJOStr, String line, String[] separator)
      throws JSONException {
    JSONObject dimensionGroupJO = new JSONObject(dimensionGroupJOStr);
    JSONArray dimensions = dimensionGroupJO.getJSONArray("d");
    if (dimensions.length() == 0)
      return true;
    int index = 0;
    for (; index < dimensions.length(); index++) {
      JSONObject dimension = dimensions.getJSONObject(index);
      String v = "";

      if (dimension.get("r") == null) {
        v = dimension.getString("v");
        separator[0] = dimensionGroupJO.getString("s");
        String[] fields = line.split(separator[0]);
        int i = dimension.getInt("i");
        if (!(v.equals(fields[i]))) {  
          return false;
        }
      }

      else if (dimension.get("i") == null) {
        String regex = dimension.getString("r");
        ArrayList<String> strList = new ArrayList<String>();
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(line);
        while (matcher.find()) {
          int start = matcher.start();
          int end = matcher.end();
          String field = line.substring(start, end);
          strList.add(field);
        }
        if (strList.size() == 0)
          return false;
        else if (strList.size() > 1)
          throw new RuntimeException("multiple matching regex result, task id:" + id + " patternStr:" + regex);
      }

      else {
        separator[0] = dimensionGroupJO.getString("s");
        String regex = dimension.getString("r");
        String[] fields = line.split(separator[0]);
        int i = dimension.getInt("i");
        String intermediateField = fields[i];

        ArrayList<String> strList = new ArrayList<String>();
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(intermediateField);

        while (matcher.find()) {
          int start = matcher.start();
          int end = matcher.end();
          String field = line.substring(start, end);
          strList.add(field);
        }

        if (strList.size() == 0)
          return false;
        else if (strList.size() > 1)
          throw new RuntimeException("multiple matching regex result, task id:" + id + " patternStr:" + regex);
      }

    }
    return true;
  }
}
