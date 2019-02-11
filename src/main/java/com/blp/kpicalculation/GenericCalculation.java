/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.blp.kpicalculation;

import com.blp.dbconnection.DBConnection;
import com.blp.ingesttenminagg.InsertTenMinuteAggregate;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author Sivakumar
 */
public class GenericCalculation {
    
    public ArrayList<HashMap<String,String>> processScanedData(String eventsData,ResultScanner resultScanner,long startDate,long endDate){
        try{
            Map<String, List<Double>> segList = new HashMap<String, List<Double>>();
            HashMap<String,String> assetSiteIdMap=new HashMap<>();
            for (Result row : resultScanner) {
                byte[] valueBytes = row.getValue(Bytes.toBytes("tag"), Bytes.toBytes(eventsData));
                if(valueBytes != null){
                    if(segList.get(Bytes.toString(row.getRow()).split("#")[2]) != null){
                        //System.out.println("Seg List Inner Loop:"+Bytes.toString(row.getRow()));
                        List<Double> valueList= segList.get(Bytes.toString(row.getRow()).split("#")[2]);
                        valueList.add(Double.valueOf(Bytes.toString(valueBytes)));
                        segList.put(Bytes.toString(row.getRow()).split("#")[2],valueList);
                        assetSiteIdMap.put(Bytes.toString(row.getRow()).split("#")[2],Bytes.toString(row.getRow()).split("#")[1]);
                    }
                    else{
                        List<Double> valueList=new ArrayList<>();
                        valueList.add(Double.valueOf(Bytes.toString(valueBytes)));
                        segList.put(Bytes.toString(row.getRow()).split("#")[2],valueList);
                        assetSiteIdMap.put(Bytes.toString(row.getRow()).split("#")[2],Bytes.toString(row.getRow()).split("#")[1]);
                    }
                }
            }
            
            
           
            
            ArrayList<HashMap<String,String>> listOfResults=new ArrayList<>();
            
            for(Map.Entry<String,List<Double>> entry : segList.entrySet()){

                HashMap<String,String> resultMap=new HashMap<>();

                double summationValue=KPICalculation.sum(entry.getValue());
                double averageSum=summationValue/entry.getValue().size();
                double max=Collections.max(entry.getValue());
                double min=Collections.min(entry.getValue());
                resultMap.put("assetId",entry.getKey());
                resultMap.put("siteId",assetSiteIdMap.get(entry.getKey()));
                resultMap.put("averageValue",Double.toString(averageSum));
                resultMap.put("max",Double.toString(max));
                resultMap.put("min",Double.toString(min));
                resultMap.put("tagName",eventsData);   
                resultMap.put("tagCounts",Integer.toString(entry.getValue().size()));
                resultMap.put("startDateTime",Long.toString(startDate));
                resultMap.put("endDateTime",Long.toString(endDate));
                
                listOfResults.add(resultMap);
            }
            
            
//            System.out.println("Size of Result grouped by asset id:"+listOfResults.size());
//            System.out.println("");
//            System.out.println(listOfResults);
//            System.out.println("");
//            
            return listOfResults;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }
    
    public ArrayList<HashMap<String,String>> processScanedDataBasedOnSiteId(String eventsData,ResultScanner resultScanner,long startDate,long endDate){
        try{
            Map<String, List<Double>> segList = new HashMap<String, List<Double>>();
            HashMap<String,String> assetSiteIdMap=new HashMap<>();
            for (Result row : resultScanner) {
                byte[] valueBytes = row.getValue(Bytes.toBytes("tag"), Bytes.toBytes(eventsData));
                if(valueBytes != null){
                    if(segList.get(Bytes.toString(row.getRow()).split("#")[2]) != null){
                        //System.out.println("Seg List Inner Loop:"+segList.get(Bytes.toString(row.getRow()).split("#")[1]));
                        List<Double> valueList= segList.get(Bytes.toString(row.getRow()).split("#")[2]);
                        valueList.add(Double.valueOf(Bytes.toString(valueBytes)));
                        segList.put(Bytes.toString(row.getRow()).split("#")[2],valueList);
                        assetSiteIdMap.put(Bytes.toString(row.getRow()).split("#")[2],Bytes.toString(row.getRow()).split("#")[1]);
                    }
                    else{
                        List<Double> valueList=new ArrayList<>();
                        valueList.add(Double.valueOf(Bytes.toString(valueBytes)));
                        segList.put(Bytes.toString(row.getRow()).split("#")[2],valueList);
                        assetSiteIdMap.put(Bytes.toString(row.getRow()).split("#")[2],Bytes.toString(row.getRow()).split("#")[1]);
                    }
                }
            }
            
            ArrayList<HashMap<String,String>> listOfResults=new ArrayList<>();

            for(Map.Entry<String,List<Double>> entry : segList.entrySet()){

                HashMap<String,String> resultMap=new HashMap<>();

                double summationValue=KPICalculation.sum(entry.getValue());
                double averageSum=summationValue/entry.getValue().size();
                
                resultMap.put("assetId",entry.getKey());
                resultMap.put("siteId",assetSiteIdMap.get(entry.getKey()));
                resultMap.put("averageValue",Double.toString(averageSum));
                resultMap.put("tagName",eventsData);              
                resultMap.put("tagCounts",Integer.toString(entry.getValue().size()));
                resultMap.put("startDateTime",Long.toString(startDate));
                resultMap.put("endDateTime",Long.toString(endDate));
                
                listOfResults.add(resultMap);
            }

            //System.out.println(listOfResults);
            //System.out.println("");
            
            return listOfResults;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }
    
    public ArrayList<HashMap<String,String>> fetchActivePower(ArrayList<HashMap<String,String>> listOfMap){
        ResultSet rsObj = null;
        Connection
        connObj = null;
        PreparedStatement pstmtObj = null;
        DBConnection jdbcObj = new DBConnection();
        try{
            DataSource dataSource = jdbcObj.setUpPool();
            connObj = dataSource.getConnection();
            DecimalFormat df = new DecimalFormat("###.#");
            
            for(HashMap<String,String> hmap: listOfMap){
                
                Statement statement = connObj.createStatement();
                float avgValue=Float.parseFloat(df.format(Double.parseDouble(hmap.get("averageValue"))));
                if(avgValue < 1){
                    avgValue=1;
                }
                
                if(avgValue > 25){
                    avgValue=25;
                }
                
                rsObj = statement.executeQuery("SELECT ACTIVEPOWER FROM masterpowercurve where SITEID='"+hmap.get("siteId")+"' and WINDSPEED='"+avgValue+"'");
                //rsObj = statement.executeQuery("SELECT ACTIVEPOWER FROM masterpowercurve where SITEID='AMB' and WINDSPEED='24.9'");
                //System.out.println(rsObj);
                
                while (rsObj.next()) {
                    if(rsObj.getObject("ACTIVEPOWER") != null){
                        //System.out.println("Double Value : "+Double.toString((Double)rsObj.getObject("ACTIVEPOWER")));
                        hmap.put("active","true");
                        hmap.put("activePower",Double.toString((Double)rsObj.getObject("ACTIVEPOWER")/6));
                    }
                    else{
                        hmap.put("active","false");
                    }
                }
                
            }
            
            //System.out.println(listOfMap);
            
            return listOfMap;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
        finally {
                try {
                        // Closing ResultSet Object
                        if(rsObj != null) {
                                rsObj.close();
                        }
                        // Closing PreparedStatement Object
                        if(pstmtObj != null) {
                                pstmtObj.close();
                        }
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
    public ArrayList<HashMap<String,String>> fetchTenMinuteAggregatedData(String dateRange,String tagName){
        ResultSet rsObj = null;
        Connection
        connObj = null;
        PreparedStatement pstmtObj = null;
        DBConnection jdbcObj = new DBConnection();
        
        ArrayList<HashMap<String,String>> mapList=new ArrayList<>();
        
        try{
            DataSource dataSource = jdbcObj.setUpPool();
            connObj = dataSource.getConnection();
            
            Statement statement = connObj.createStatement();
            rsObj = statement.executeQuery("select site_id,asset_id,tag_name,sum(agg_value) as agg_value,max(agg_value) as max_agg_value,min(agg_value) as min_agg_value from ten_min_agg where Date(from_ts)='"+dateRange+"' and tag_name='"+tagName+"' group by asset_id");


            while (rsObj.next()) {
                
                String site_id=(String)rsObj.getObject("site_id");
                String assetId=(String)rsObj.getObject("asset_id");
                String tag_name=(String)rsObj.getObject("tag_name");
                String dateInString=dateRange;
                double avgValue=(Double)rsObj.getObject("agg_value");
                double maxValue=(Double)rsObj.getObject("max_agg_value");
                double minValue=(Double)rsObj.getObject("min_agg_value");
                
                HashMap<String,String> hashMap=new HashMap<String,String>();
                
                hashMap.put("siteId",site_id);
                hashMap.put("assetId", assetId);
                hashMap.put("tagName", tag_name);
                hashMap.put("dateInString", dateInString);
                hashMap.put("aggValue", Double.toString(avgValue));
                hashMap.put("max_value", Double.toString(maxValue));
                hashMap.put("min_value", Double.toString(minValue));
                
                mapList.add(hashMap);
            }
            
            System.out.println(mapList);
            
            
            new InsertTenMinuteAggregate().insertDailyAggregate(mapList);
            
            return null;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
        finally {
                try {
                        // Closing ResultSet Object
                        if(rsObj != null) {
                                rsObj.close();
                        }
                        // Closing PreparedStatement Object
                        
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
    public ArrayList<HashMap<String,String>> fetchTenMinuteGenerationAggregatedData(String dateRange,String tagName){
        ResultSet rsObj = null;
        Connection
        connObj = null;
        PreparedStatement pstmtObj = null;
        DBConnection jdbcObj = new DBConnection();
        
        ArrayList<HashMap<String,String>> mapList=new ArrayList<>();
        
        try{
            DataSource dataSource = jdbcObj.setUpPool();
            connObj = dataSource.getConnection();
            
            Statement statement = connObj.createStatement();
            rsObj = statement.executeQuery("select site_id,asset_id,tag_name,sum(agg_value) as agg_value,max(agg_value) as max_agg_value,min(agg_value) as min_agg_value from ten_min_agg where Date(from_ts)='"+dateRange+"' and tag_name='"+tagName+"' group by asset_id");


            while (rsObj.next()) {
                
                String site_id=(String)rsObj.getObject("site_id");
                String assetId=(String)rsObj.getObject("asset_id");
                String tag_name=(String)rsObj.getObject("tag_name");
                String dateInString=dateRange;
                double avgValue=(Double)rsObj.getObject("agg_value");
                
                HashMap<String,String> hashMap=new HashMap<String,String>();
                
                hashMap.put("siteId",site_id);
                hashMap.put("assetId", assetId);
                hashMap.put("tagName", tag_name);
                hashMap.put("date", dateInString);
                hashMap.put("generationValue", Double.toString(avgValue));
                
                mapList.add(hashMap);
            }
            
            System.out.println(mapList);
            
            
            new InsertTenMinuteAggregate().insertDailyGenerationAggregate(mapList);
            
            return null;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
        finally {
                try {
                        // Closing ResultSet Object
                        if(rsObj != null) {
                                rsObj.close();
                        }
                        // Closing PreparedStatement Object
                        
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
    
    public ArrayList<HashMap<String,String>> processScanedDataForGeneration(String eventsData,ResultScanner resultScanner,long startDate,long endDate,String calcType){
        try{
            Map<String, List<Double>> segList = new HashMap<String, List<Double>>();
            HashMap<String,String> assetSiteIdMap=new HashMap<>();
            for (Result row : resultScanner) {
                byte[] valueBytes = row.getValue(Bytes.toBytes("tag"), Bytes.toBytes(eventsData));
                if(valueBytes != null){
                    if(segList.get(Bytes.toString(row.getRow()).split("#")[2]) != null){
                        //System.out.println("Seg List Inner Loop:"+segList.get(Bytes.toString(row.getRow()).split("#")[1]));
                        List<Double> valueList= segList.get(Bytes.toString(row.getRow()).split("#")[2]);
                        valueList.add(Double.valueOf(Bytes.toString(valueBytes)));
                        segList.put(Bytes.toString(row.getRow()).split("#")[2],valueList);
                        assetSiteIdMap.put(Bytes.toString(row.getRow()).split("#")[2],Bytes.toString(row.getRow()).split("#")[1]);
                    }
                    else{
                        List<Double> valueList=new ArrayList<>();
                        valueList.add(Double.valueOf(Bytes.toString(valueBytes)));
                        segList.put(Bytes.toString(row.getRow()).split("#")[2],valueList);
                        assetSiteIdMap.put(Bytes.toString(row.getRow()).split("#")[2],Bytes.toString(row.getRow()).split("#")[1]);
                    }
                }
            }
            
            
           
            
            ArrayList<HashMap<String,String>> listOfResults=new ArrayList<>();
            
            for(Map.Entry<String,List<Double>> entry : segList.entrySet()){

                HashMap<String,String> resultMap=new HashMap<>();
                
                double maxORmin=0;
                if(calcType.equalsIgnoreCase("max")){
                    maxORmin=Collections.max(entry.getValue());
                }
                else{
                    maxORmin=Collections.min(entry.getValue());
                }
                
                
                resultMap.put("assetId",entry.getKey());
                resultMap.put("siteId",assetSiteIdMap.get(entry.getKey()));
                resultMap.put(calcType,Double.toString(maxORmin));
                
                
                listOfResults.add(resultMap);
            }
            
            
//            System.out.println("Size of Result grouped by asset id:"+listOfResults.size());
//            System.out.println("");
//           System.out.println(listOfResults);
//            System.out.println("");
//            
            return listOfResults;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }
    
}
