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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.joda.time.DateTime;

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
            rsObj = statement.executeQuery("select * from 24hrs_windspeed_avg_data where ASSET_NAME='SACU_WT01'");
            
            while (rsObj.next()) {
                List<Double> valueList=new ArrayList<>();
                for(int i=0;i<=23;i++){
                    double hourValue=0;
                    if(i < 10){
                        hourValue=(Double)rsObj.getObject("0"+i+"");
                    }
                    else{
                        hourValue=(Double)rsObj.getObject(""+i+"");
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
                System.out.println("Value List before adding: "+valueList);
                System.out.println("Value List size before removiing: "+valueList.size());
                valueList.remove(0);
                System.out.println("Value List size after removiing: "+valueList.size());
                
                valueList.add(value);
                System.out.println("Value List size after adding: "+valueList.size());
                System.out.println("Value List after adding: "+valueList);
                
                sql="update 24hrs_windspeed_avg_data set ";
                for(int i=0;i < valueList.size();i++){
                    if(i < 10){
                        sql=sql+"0"+i+"= "+valueList.get(i)+",";
                    }
                    else if(i == 23){
                        sql=sql+i+"= "+valueList.get(i);
                    }
                    else{
                        sql=sql+i+"= "+valueList.get(i)+",";
                    }
                    
                }
                
                sql=sql+" where ASSET_NAME='"+entry.getKey()+"';";
            }
            
            System.out.println("SQL : "+sql);
            /*connObj = dataSource.getConnection();
            statement = connObj.createStatement();
            
            statement.executeUpdate(sql);*/
            
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
    
    public String calculate24HoursWindSpeedAggData(){
        ResultSet rsObj = null;
        Connection connObj = null;
        DBConnection jdbcObj = new DBConnection();
        try{
            DataSource dataSource = jdbcObj.setUpPool();
            connObj = dataSource.getConnection();
            jdbcObj.printDbStatus();
            
            Statement statement = connObj.createStatement();
            rsObj = statement.executeQuery("SELECT ASSET_ID, AVG(AGG_VALUE) FROM m2m_blp.ten_min_agg where from_ts >= '2018-05-02 16:00:00' and to_ts <= '2018-05-02 17:00:00' and asset_id = 'AMB_GA05' and tag_name = 'WINDSPEED';");
            
            while (rsObj.next()) {
                
            }
            
            return null;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }
    
}
