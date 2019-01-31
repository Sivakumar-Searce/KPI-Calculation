/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.blp.ingesttenminagg;

import com.blp.dbconnection.DBConnection;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import javax.sql.DataSource;

/**
 *
 * @author siva.kumar
 */
public class InsertTenMinuteAggregate {
    public String insertTenMinuteAggregate(ArrayList<HashMap<String,String>> listOfAvgValues){
        Connection connObj = null;
        DBConnection jdbcObj = new DBConnection();
        try{
            DataSource dataSource = jdbcObj.setUpPool();
            connObj = dataSource.getConnection();
            
            for(HashMap<String,String> dataMap:listOfAvgValues){
                
                String assetId=dataMap.get("assetId");
                String siteID=dataMap.get("siteId");
                String averageValue=dataMap.get("averageValue");
                String tagName=dataMap.get("tagName");
                double max=Double.parseDouble(dataMap.get("max"));
                double min=Double.parseDouble(dataMap.get("min"));
                String startDateTime=dataMap.get("startDateTime");
                String endDateTime=dataMap.get("endDateTime");
                
                Date startDate = new Date(Long.parseLong(startDateTime)*1000);
                Date endDate = new Date(Long.parseLong(endDateTime)*1000);
                
                java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                
                Statement statement = connObj.createStatement();

                statement.executeUpdate("insert into ten_min_agg(site_id,asset_id,tag_name,from_ts,to_ts,agg_value,min_value,max_value) values('"+siteID+"','"+assetId+"',"
                        + "'"+tagName+"','"+sdf.format(startDate)+"','"+sdf.format(endDate)+"',"+averageValue+",'"+min+"','"+max+"')");
                
            }
            
            return null;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
        finally {
                try {
                    // Closing Connection Object
                    if(connObj != null) {
                            connObj.close();
                    }
                } catch(Exception sqlException) {
                        sqlException.printStackTrace();
                }
                jdbcObj.printDbStatus();
            }
    }
    public String insertTenMinuteAggregateForPotental(ArrayList<HashMap<String,String>> listOfAvgValues){
        Connection connObj = null;
        DBConnection jdbcObj = new DBConnection();
        try{
            DataSource dataSource = jdbcObj.setUpPool();
            connObj = dataSource.getConnection();
            
            for(HashMap<String,String> dataMap:listOfAvgValues){
                
                if(dataMap.get("active").equalsIgnoreCase("true")){
                    String assetId=dataMap.get("assetId");
                    String siteID=dataMap.get("siteId");
                    String averageValue=dataMap.get("activePower");
                    String tagName="PotentialProduction";
                    String startDateTime=dataMap.get("startDateTime");
                    String endDateTime=dataMap.get("endDateTime");

                    Date startDate = new Date(Long.parseLong(startDateTime)*1000);
                    Date endDate = new Date(Long.parseLong(endDateTime)*1000);

                    java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    Statement statement = connObj.createStatement();

                    statement.executeUpdate("insert into ten_min_agg(site_id,asset_id,tag_name,from_ts,to_ts,agg_value) values('"+siteID+"','"+assetId+"',"
                            + "'"+tagName+"','"+sdf.format(startDate)+"','"+sdf.format(endDate)+"',"+averageValue+")");
                }
            }
            
            return null;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
        finally {
                try {
                    // Closing Connection Object
                    if(connObj != null) {
                            connObj.close();
                    }
                } catch(Exception sqlException) {
                        sqlException.printStackTrace();
                }
                jdbcObj.printDbStatus();
            }
    }
    public String insertDailyAggregate(ArrayList<HashMap<String,String>> listOfAvgValues){
        Connection connObj = null;
        DBConnection jdbcObj = new DBConnection();
        try{
            DataSource dataSource = jdbcObj.setUpPool();
            connObj = dataSource.getConnection();
            
            for(HashMap<String,String> dataMap:listOfAvgValues){
                
                String assetId=dataMap.get("assetId");
                String siteID=dataMap.get("siteId");
                double averageValue=Double.parseDouble(dataMap.get("aggValue"));
                String tagName=dataMap.get("tagName");
                String dateInString=dataMap.get("dateInString");
                double max_value=Double.parseDouble(dataMap.get("max_value"));
                double min_value=Double.parseDouble(dataMap.get("min_value"));
                
                Statement statement = connObj.createStatement();

                statement.executeUpdate("insert into daily_agg(site_id,asset_id,tag_name,date_agg,agg_value,max_value,min_value) values('"+siteID+"','"+assetId+"',"
                        + "'"+tagName+"','"+dateInString+"',"+averageValue+","+max_value+","+min_value+")");
                
            }
            
            return null;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
        finally {
                try {
                    // Closing Connection Object
                    if(connObj != null) {
                            connObj.close();
                    }
                } catch(Exception sqlException) {
                        sqlException.printStackTrace();
                }
                jdbcObj.printDbStatus();
            }
    }
    
    public String insertDailyGenerationAggregate(ArrayList<HashMap<String,String>> listOfAvgValues){
        Connection connObj = null;
        DBConnection jdbcObj = new DBConnection();
        try{
            DataSource dataSource = jdbcObj.setUpPool();
            connObj = dataSource.getConnection();
            
            for(HashMap<String,String> dataMap:listOfAvgValues){
                
                String assetId=dataMap.get("assetId");
                String siteID=dataMap.get("siteId");
                double averageValue=Double.parseDouble(dataMap.get("generationValue"));
                String tagName="Generation";
                String dateInString=dataMap.get("date");
                
                Statement statement = connObj.createStatement();

                statement.executeUpdate("insert into daily_agg(site_id,asset_id,tag_name,date_agg,agg_value) values('"+siteID+"','"+assetId+"',"
                        + "'"+tagName+"','"+dateInString+"',"+averageValue+")");
                
            }
            
            return null;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
        finally {
                try {
                    // Closing Connection Object
                    if(connObj != null) {
                            connObj.close();
                    }
                } catch(Exception sqlException) {
                        sqlException.printStackTrace();
                }
                jdbcObj.printDbStatus();
            }
    }
    public String insertTenMinuteAggregateForSOK(ArrayList<HashMap<String,String>> listOfAvgValues){
        Connection connObj = null;
        DBConnection jdbcObj = new DBConnection();
        try{
            DataSource dataSource = jdbcObj.setUpPool();
            connObj = dataSource.getConnection();
            
            for(HashMap<String,String> dataMap:listOfAvgValues){
                
                String assetId=dataMap.get("assetId");
                String siteID=dataMap.get("siteId");
                String averageValue=dataMap.get("uptime");
                String tagName=dataMap.get("tagName");
                String startDateTime=dataMap.get("startDate");
                String endDateTime=dataMap.get("endDate");
                
                Date startDate = new Date(Long.parseLong(startDateTime)*1000);
                Date endDate = new Date(Long.parseLong(endDateTime)*1000);
                
                java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                
                Statement statement = connObj.createStatement();

                statement.executeUpdate("insert into ten_min_agg(site_id,asset_id,tag_name,from_ts,to_ts,agg_value) values('"+siteID+"','"+assetId+"',"
                        + "'"+tagName+"','"+sdf.format(startDate)+"','"+sdf.format(endDate)+"',"+averageValue+")");
                
            }
            
            return null;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
        finally {
                try {
                    // Closing Connection Object
                    if(connObj != null) {
                            connObj.close();
                    }
                } catch(Exception sqlException) {
                        sqlException.printStackTrace();
                }
                jdbcObj.printDbStatus();
            }
    }
}
