/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.blp.kpicalculation;

import com.blp.dbconnection.DBConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author Hemanth
 */
public class GenericCalculation {
    
    public ArrayList<HashMap<String,String>> processScanedData(String eventsData,ResultScanner resultScanner,long startDate,long endDate){
        try{
            Map<String, List<Double>> segList = new HashMap<String, List<Double>>();
            int i=0;
            for (Result row : resultScanner) {
                byte[] valueBytes = row.getValue(Bytes.toBytes("tag"), Bytes.toBytes(eventsData));
                if(valueBytes != null){
                    if(segList.get(Bytes.toString(row.getRow()).split("#")[1]) != null){
                        //System.out.println("Seg List Inner Loop:"+segList.get(Bytes.toString(row.getRow()).split("#")[1]));
                        List<Double> valueList= segList.get(Bytes.toString(row.getRow()).split("#")[1]);
                        valueList.add(Double.valueOf(Bytes.toString(valueBytes)));
                        segList.put(Bytes.toString(row.getRow()).split("#")[1],valueList);
                        i++;
                    }
                    else{
                        List<Double> valueList=new ArrayList<>();
                        valueList.add(Double.valueOf(Bytes.toString(valueBytes)));
                        segList.put(Bytes.toString(row.getRow()).split("#")[1],valueList);
                        i++;
                    }
                }
            }
            
            
            System.out.println("Total No. of BigTable rows scanned:"+i);
            System.out.println("");
            
            ArrayList<HashMap<String,String>> listOfResults=new ArrayList<>();
            
            for(Map.Entry<String,List<Double>> entry : segList.entrySet()){

                HashMap<String,String> resultMap=new HashMap<>();

                double summationValue=KPICalculation.sum(entry.getValue());
                double averageSum=summationValue/entry.getValue().size();

                resultMap.put("assetId",entry.getKey());
                resultMap.put("averageValue",Double.toString(averageSum));
                resultMap.put("tagName",eventsData);   
                resultMap.put("tagCounts",Integer.toString(entry.getValue().size()));
                resultMap.put("startDateTime",Long.toString(startDate));
                resultMap.put("endDateTime",Long.toString(endDate));
                
                listOfResults.add(resultMap);
            }
            
            
            System.out.println("Size of Result grouped by asset id:"+listOfResults.size());
            System.out.println("");
            System.out.println(listOfResults);
            System.out.println("");
            
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
                    if(segList.get(Bytes.toString(row.getRow()).split("#")[1]) != null){
                        //System.out.println("Seg List Inner Loop:"+segList.get(Bytes.toString(row.getRow()).split("#")[1]));
                        List<Double> valueList= segList.get(Bytes.toString(row.getRow()).split("#")[1]);
                        valueList.add(Double.valueOf(Bytes.toString(valueBytes)));
                        segList.put(Bytes.toString(row.getRow()).split("#")[1],valueList);
                        assetSiteIdMap.put(Bytes.toString(row.getRow()).split("#")[1],Bytes.toString(row.getRow()).split("#")[2]);
                    }
                    else{
                        List<Double> valueList=new ArrayList<>();
                        valueList.add(Double.valueOf(Bytes.toString(valueBytes)));
                        segList.put(Bytes.toString(row.getRow()).split("#")[1],valueList);
                        assetSiteIdMap.put(Bytes.toString(row.getRow()).split("#")[1],Bytes.toString(row.getRow()).split("#")[2]);
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

            System.out.println(listOfResults);
            System.out.println("");
            
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
                ResultSetMetaData rsmd = rsObj.getMetaData();
                //System.out.println(rsObj.getRow());
                int columnsNumber = rsmd.getColumnCount();
                //System.out.println(columnsNumber);
                while (rsObj.next()) {
                    for (int i = 1    ; i <= columnsNumber; i++) {
                        if (i > 1) System.out.print(",  ");
                        String columnValue = rsObj.getString(i);
                        System.out.print(hmap.get("siteId")+" : "+rsmd.getColumnName(i)+" : "+columnValue);
                    }
                    System.out.println("");
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
    
}
