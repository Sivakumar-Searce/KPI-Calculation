/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.blp.dbconnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

/**
 *
 * @author siva.kumar
 */
public class DBUtils {
    public String fetchModuleName(String assetId){
        ResultSet rsObj = null;
        Connection connObj = null;
        DBConnection jdbcObj = new DBConnection();
        
        try{
            DataSource dataSource = jdbcObj.setUpPoolPulseTest();
            jdbcObj.printDbStatus();

            // Performing Database Operation!
            System.out.println("\n=====Making A New Connection Object For Db Transaction=====\n");
            connObj = dataSource.getConnection();
            jdbcObj.printDbStatus();

            Statement statement = connObj.createStatement();
            rsObj = statement.executeQuery("SELECT datafield6 FROM core_asset_details where asset_name='"+assetId+"'");

            String dataField=null;
            while (rsObj.next()) {
                dataField=(String)rsObj.getObject("datafield6");
                dataField=dataField.replace("-","_").replace(".","").toLowerCase();
            }
            
            System.out.println("DataFeild : "+dataField);
            
            if(dataField != null){
                return dataField;
            }
            
            return null;
        }
        catch(Exception e){
            return null;
        }
        finally {
            try {
                    // Closing ResultSet Object
                    if(rsObj != null) {
                            rsObj.close();
                    }

                    // Closing Connection Object
                    if(connObj != null) {
                            connObj.close();
                    }
            } catch(Exception sqlException) {
                    sqlException.printStackTrace();
            }
        }
    }
    public ArrayList<HashMap<String,String>> fetchalarmcode(String moduleName){
        ResultSet rsObj = null;
        Connection connObj = null;
        DBConnection jdbcObj = new DBConnection();
        
        ArrayList<HashMap<String,String>> mapList=new ArrayList<>();
        try{
            DataSource dataSource = jdbcObj.setUpPoolMidas();
            jdbcObj.printDbStatus();

            // Performing Database Operation!
            System.out.println("\n=====Making A New Connection Object For Db Transaction=====\n");
            connObj = dataSource.getConnection();
            jdbcObj.printDbStatus();

            Statement statement = connObj.createStatement();
            rsObj = statement.executeQuery("SELECT faultstate,alarm_code,alarm_cat FROM "+moduleName+"");
            //Uncomment the above line
            //rsObj = statement.executeQuery("SELECT faultstate,alarm_code,alarm_cat FROM g58_850mw");
            
            while (rsObj.next()) {
                String alarmCategory=(String)rsObj.getObject("alarm_cat");
                int alarmCode=(Integer)rsObj.getObject("alarm_code");
                int faultState=(Integer)rsObj.getObject("faultstate");
                //System.out.println("Values :"+alarmCategory+" : "+alarmCode+" : "+faultState);
                
                HashMap<String,String> alarmCodeMap=new HashMap<>();
                
                alarmCodeMap.put("alarmCategory", alarmCategory);
                alarmCodeMap.put("alarmCode", Integer.toString(alarmCode));
                alarmCodeMap.put("faultState", Integer.toString(faultState));
                
                mapList.add(alarmCodeMap);
            }
            
            //System.out.println("Map List:"+mapList);
            
            return mapList;
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

                    // Closing Connection Object
                    if(connObj != null) {
                            connObj.close();
                    }
                } catch(Exception sqlException) {
                        sqlException.printStackTrace();
                }
            }
    }
    public ArrayList<HashMap<String,String>> fecthActivePowerForFFCCalculation(String startTimeStamp, String endTimeStamp,String assetId,String comparision,String activeCount,String faultValue){
        ResultSet rsObj = null;
        Connection connObj = null;
        DBConnection jdbcObj = new DBConnection();
        ArrayList<HashMap<String,String>> mapList=new ArrayList<>();
        ArrayList<HashMap<String,String>> resultMapList=new ArrayList<>();
        try{
            
            DataSource dataSource = jdbcObj.setUpPool();
            jdbcObj.printDbStatus();

            // Performing Database Operation!
            System.out.println("\n=====Making A New Connection Object For Db Transaction=====\n");
            connObj = dataSource.getConnection();
            jdbcObj.printDbStatus();
            
            Statement statement = connObj.createStatement();
            rsObj = statement.executeQuery("SELECT * FROM ten_min_agg where from_ts >= '"+startTimeStamp+"' and to_ts <='"+endTimeStamp+"' and asset_id='"+assetId+"' and tag_name='ActivePower'");
            
            while (rsObj.next()) {
                HashMap<String,String> hmap=new HashMap<>();
                hmap.put("avgValue",(String)rsObj.getObject("avg_value"));
                hmap.put("assetId",(String)rsObj.getObject("asset_id"));
                hmap.put("max",Double.toString((Double)rsObj.getObject("max_value")));
                hmap.put("min",Double.toString((Double)rsObj.getObject("min_value")));
                hmap.put("startTimeStamp",(String)rsObj.getObject("from_ts"));
                hmap.put("endTimeStamp",(String)rsObj.getObject("to_ts"));
                mapList.add(hmap);
            }
            System.out.println(mapList);
            int activeIterationCount=Integer.parseInt(activeCount);
            ArrayList<Double> doubleList=new ArrayList<>();
            
            if(comparision.equalsIgnoreCase("normal")){
                for(HashMap<String,String> hmap:mapList){

                    double avgValue=Double.parseDouble(hmap.get("avgValue"));
                    if(avgValue > Double.parseDouble(faultValue)){
                        doubleList.add(avgValue);
                    }
                    else{
                        doubleList.clear();
                    }
                    if(doubleList.size() == activeIterationCount){
                        HashMap<String,String> faultMap=new HashMap<>();
                        faultMap.put("assetId", hmap.get("assetId"));
                        faultMap.put("endTimeStamp", hmap.get("endTimeStamp"));
                        faultMap.put("faultValue", "0");
                        
                        resultMapList.add(faultMap);
                    }

                }
            }
            else if(comparision.equalsIgnoreCase("min")){
                for(HashMap<String,String> hmap:mapList){

                    double avgValue=Double.parseDouble(hmap.get("min_value"));
                    if(avgValue > Double.parseDouble(faultValue)){
                        doubleList.add(avgValue);
                    }
                    else{
                        doubleList.clear();
                    }
                    if(doubleList.size() == activeIterationCount){
                        HashMap<String,String> faultMap=new HashMap<>();
                        faultMap.put("assetId", hmap.get("assetId"));
                        faultMap.put("endTimeStamp", hmap.get("endTimeStamp"));
                        faultMap.put("faultValue", "0");
                        
                        resultMapList.add(faultMap);
                    }

                }
            }
            
            System.out.println("Fault Map: "+resultMapList);
            
            return resultMapList;
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

                    // Closing Connection Object
                    if(connObj != null) {
                            connObj.close();
                    }
                } catch(Exception sqlException) {
                        sqlException.printStackTrace();
                }
            }
    }
    public String fetchToUpdate24HoursWindSpeedAggData(double value,String assetName){
        ResultSet rsObj = null;
        Connection connObj = null;
        DBConnection jdbcObj = new DBConnection();
        try{
            DataSource dataSource = jdbcObj.setUpPoolPulseTest();
            //jdbcObj.printDbStatus();
            
            Map<String, List<Double>> aggDataList = new HashMap<>();
            
            connObj = dataSource.getConnection();
            jdbcObj.printDbStatus();
            
            Statement statement = connObj.createStatement();
            rsObj = statement.executeQuery("select * from 24hrs_windspeed_avg_data where ASSET_NAME='"+assetName+"'");
            
            while (rsObj.next()) {
                List<Double> valueList=new ArrayList<>();
                for(int i=0;i<=23;i++){
                    double hourValue=0;
                    if(i < 10){
                        hourValue=(double)rsObj.getObject("0"+i+"");
                    }
                    else{
                        hourValue=(double)rsObj.getObject(""+i+"");
                    }
                    valueList.add(hourValue);
                }
                String assetNameInDB=(String)rsObj.getObject("ASSET_NAME");
                aggDataList.put(assetNameInDB,valueList);
            }
            rsObj.close();
            connObj.close();
            
            String sql=null;
            for(Map.Entry<String,List<Double>> entry : aggDataList.entrySet()){
                List<Double> valueList=entry.getValue();
                /*System.out.println("Value List before adding: "+valueList);
                System.out.println("Value List size before removiing: "+valueList.size());*/
                valueList.remove(0);
                //System.out.println("Value List size after removiing: "+valueList.size());
                
                valueList.add(value);
                //System.out.println("Value List size after adding: "+valueList.size());
                //System.out.println("Value List after adding: "+valueList);
                
                sql="update 24hrs_windspeed_avg_data set ";
                for(int i=0;i < valueList.size();i++){
                    if(i < 10){
                        sql=sql+"`0"+i+"`= "+valueList.get(i)+",";
                    }
                    else if(i == 23){
                        sql=sql+"`"+i+"`= "+valueList.get(i);
                    }
                    else{
                        sql=sql+"`"+i+"`= "+valueList.get(i)+",";
                    }
                    
                }
                
                sql=sql+" where ASSET_NAME='"+entry.getKey()+"';";
            }
            
            System.out.println("SQL : "+sql);
            
            connObj = dataSource.getConnection();
            statement = connObj.createStatement();
            
            statement.executeUpdate(sql);
            
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

                // Closing Connection Object
                if(connObj != null) {
                        connObj.close();
                }
            } catch(Exception sqlException) {
                    sqlException.printStackTrace();
            }
        }
    }
    
    public ArrayList<HashMap<String,String>> calculate24HoursWindSpeedAggData(String startDate,String endDate){
        ResultSet rsObj = null;
        Connection connObj = null;
        DBConnection jdbcObj = new DBConnection();
        try{
            
            List<String> assetList=new DBUtils().fetchListOfAssetNames();
            
            ArrayList<HashMap<String,String>> averageList=new ArrayList<>();
            
            DataSource dataSource = jdbcObj.setUpPool();
            connObj = dataSource.getConnection();
            jdbcObj.printDbStatus();
            
            for(String assetId:assetList){
            
                Statement statement = connObj.createStatement();
                rsObj = statement.executeQuery("SELECT ASSET_ID, AVG(AGG_VALUE) as agg_value FROM m2m_blp.ten_min_agg where from_ts >= '2018-05-02 16:00:00' and to_ts <= '2018-05-02 17:00:00' and asset_id ='"+assetId+"' and tag_name = 'WINDSPEED';");

                while (rsObj.next()) {
                    HashMap<String,String> dataMap=new HashMap<>();
                    if(rsObj.getObject("ASSET_ID") != null && rsObj.getObject("agg_value") != null){
                        System.out.println((String)rsObj.getObject("ASSET_ID") +" : "+(Double)rsObj.getObject("agg_value"));
                        dataMap.put("assetId",(String)rsObj.getObject("ASSET_ID"));
                        dataMap.put("aggValue",Double.toString((Double)rsObj.getObject("agg_value")));

                        averageList.add(dataMap);
                    }
                }
            }
            System.out.println("Average List : "+averageList);
            for(HashMap<String,String> dataMap:averageList){
                new DBUtils().fetchToUpdate24HoursWindSpeedAggData(Double.parseDouble(dataMap.get("aggValue")),dataMap.get("assetId"));
            }
            
            return averageList;
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

                // Closing Connection Object
                if(connObj != null) {
                        connObj.close();
                }
            } catch(Exception sqlException) {
                    sqlException.printStackTrace();
            }
        }
    }
    public ArrayList<String> fetchListOfAssetNames(){
        ResultSet rsObj = null;
        Connection connObj = null;
        DBConnection jdbcObj = new DBConnection();
        ArrayList<String> resultList=new ArrayList<>();
        try{
            DataSource dataSource = jdbcObj.setUpPoolPulseTest();
            connObj = dataSource.getConnection();
            jdbcObj.printDbStatus();
            
            Statement statement = connObj.createStatement();
            rsObj = statement.executeQuery("SELECT asset_name FROM oriondb_pulse_test.24hrs_windspeed_avg_data;");
            
            while (rsObj.next()) {
                resultList.add((String)rsObj.getObject("asset_name"));
            }
            
            return resultList;
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

                // Closing Connection Object
                if(connObj != null) {
                        connObj.close();
                }
            } catch(Exception sqlException) {
                    sqlException.printStackTrace();
            }
        }
    }
    
    public ArrayList<HashMap<String,String>> fetchTenMinuteAggregates(String startDate, String endDate,String tagName){
        ResultSet rsObj = null;
        Connection connObj = null;
        DBConnection jdbcObj = new DBConnection();
        try{
            DataSource dataSource= jdbcObj.setUpPool();
            
            ArrayList<HashMap<String,String>> listMap=new ArrayList<>();
            
            connObj = dataSource.getConnection();
            jdbcObj.printDbStatus();
            
            Statement statement = connObj.createStatement();
            rsObj = statement.executeQuery("select site_id,asset_id,tag_name,agg_value,from_ts,to_ts from ten_min_agg where from_ts='"+startDate+"' and to_ts='"+endDate+"' and tag_name='"+tagName+"'");
            
            while (rsObj.next()) {
                String site_id=(String)rsObj.getObject("site_id");
                String assetId=(String)rsObj.getObject("asset_id");
                String tag_name=(String)rsObj.getObject("tag_name");
                double avgValue=(Double)rsObj.getObject("agg_value");
                
                HashMap<String,String> hmap=new HashMap<>();
                hmap.put("siteId",site_id);
                hmap.put("assetId",assetId);
                hmap.put("tagName",tag_name);
                hmap.put("avgValue",Double.toString(avgValue));
                hmap.put("startDate",(String)rsObj.getObject("from_ts"));
                hmap.put("endDate",(String)rsObj.getObject("to_ts"));
                
                listMap.add(hmap);
            }
            
            return listMap;
        }
        catch(Exception e){
            return null;
        }
        finally {
            try {
                    // Closing ResultSet Object
                if(rsObj != null) {
                        rsObj.close();
                }

                // Closing Connection Object
                if(connObj != null) {
                        connObj.close();
                }
            } catch(Exception sqlException) {
                    sqlException.printStackTrace();
            }
        }
    }
    
    public ArrayList<HashMap<String,String>> fetchSumOfTenMinuteAggregatesSOK(String startDate, String endDate,String tagName){
        ResultSet rsObj = null;
        Connection connObj = null;
        DBConnection jdbcObj = new DBConnection();
        try{
            DataSource dataSource= jdbcObj.setUpPool();
            
            ArrayList<HashMap<String,String>> listMap=new ArrayList<>();
            
            connObj = dataSource.getConnection();
            jdbcObj.printDbStatus();
            
            Statement statement = connObj.createStatement();
            rsObj = statement.executeQuery("select site_id,asset_id,tag_name,sum(agg_value) as agg_value from m2m_blp.ten_min_agg where from_ts >= '"+startDate+"' and to_ts < '"+endDate+"'  and tag_name='"+tagName+"' group by asset_id;");
            
            while (rsObj.next()) {
                String site_id=(String)rsObj.getObject("site_id");
                String assetId=(String)rsObj.getObject("asset_id");
                String tag_name=(String)rsObj.getObject("tag_name");
                double avgValue=(Double)rsObj.getObject("agg_value");
                
                HashMap<String,String> hmap=new HashMap<>();
                hmap.put("siteId",site_id);
                hmap.put("assetId",assetId);
                hmap.put("tagName",tag_name);
                hmap.put("avgValue",Double.toString(avgValue/86400));
                hmap.put("startDate",startDate);
                hmap.put("endDate",endDate);
                
                listMap.add(hmap);
            }
            
            return listMap;
        }
        catch(Exception e){
            return null;
        }
        finally {
            try {
                    // Closing ResultSet Object
                if(rsObj != null) {
                        rsObj.close();
                }

                // Closing Connection Object
                if(connObj != null) {
                        connObj.close();
                }
            } catch(Exception sqlException) {
                    sqlException.printStackTrace();
            }
        }
    }
    
}
