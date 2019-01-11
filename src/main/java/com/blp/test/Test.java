/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.blp.test;

import com.blp.dbconnection.DBConnection;
import com.blp.kpicalculation.GenericCalculation;
import com.blp.kpicalculation.KPICalculation;
import static com.blp.kpicalculation.KPICalculation.instanceId;
import static com.blp.kpicalculation.KPICalculation.projectId;
import static com.blp.kpicalculation.KPICalculation.tableId;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author siva.kumar
 */
public class Test {
    public String genericFunctionForKPI(){
        try{
            
            byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("tag");
            byte[] COLUMN_NAME = Bytes.toBytes("AlarmCode");
            
            try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
            
                
                Scan scan = new Scan();
                
                Table table = connection.getTable(TableName.valueOf(tableId));

                ResultScanner scanner = table.getScanner(scan);
                
                
                ArrayList<HashMap<String,String>> hmapList=new ArrayList<HashMap<String,String>>();
                
                for (Result row : scanner) {
                    byte[] valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
                    if(valueBytes != null){
                        
                            int alarm=(int)Double.parseDouble(Bytes.toString(valueBytes));
                            HashMap<String,String> innerMap=new HashMap();
                            innerMap.put("timestamp", Bytes.toString(row.getRow()).split("#")[0]);
                            innerMap.put("alarmcode", Integer.toString(alarm));
                            innerMap.put("siteId", Bytes.toString(row.getRow()).split("#")[2]);
                            innerMap.put("assetId", Bytes.toString(row.getRow()).split("#")[1]);
                            hmapList.add(innerMap);
                       
                    }
                }
                
                /*for (Result row : scanner) {
                    byte[] valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
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
                }*/

                //System.out.println(hmapList);
                
                ArrayList<HashMap<String,String>> eventsCategoryList=new Test().fetchAlarmCategory();
                
                ArrayList<HashMap<String,String>> filteredList=new ArrayList<>();
                
                for (HashMap<String,String> dataMap: hmapList){
                    
                    String siteId=dataMap.get("siteId");
                    if(siteId.equalsIgnoreCase("sutdac")){
                        siteId="SUT";
                    }
                    String alarmCode=dataMap.get("alarmcode");
                    //System.out.println(siteId+":"+alarmCode);
                    for(HashMap<String,String> categoryMap:eventsCategoryList){
//                        System.out.println("----------------");
//                        System.out.println("Inner Loop: "+categoryMap.get("siteId")+":"+categoryMap.get("eventCode"));
//                        System.out.println("----------------");
                        if(categoryMap.get("siteId").equalsIgnoreCase(siteId) && categoryMap.get("eventCode").equalsIgnoreCase(alarmCode)){
                            
                            HashMap<String,String> finalMap=new HashMap<>();
                            
                            finalMap.put("assetId", dataMap.get("siteId"));
                            finalMap.put("siteId", dataMap.get("siteId"));
                            finalMap.put("timestamp", dataMap.get("timestamp"));
                            finalMap.put("eventCode", categoryMap.get("eventCode"));
                            finalMap.put("category", categoryMap.get("category"));
                            finalMap.put("firstFault",categoryMap.get("firstFault"));
                            
                            filteredList.add(finalMap);
                        }
                    }
                    
                }
                
                System.out.println(filteredList);
                
                
                
            }   
            catch(Exception e){
                e.printStackTrace();
            }
            
            return "success";
        }
        catch(Exception e){
            return null;
        }
    }
    public static void main(String args[])
    {
        //new Test().genericFunctionForKPI();
        //new Test().fetchAlarmCategory();
        //Test.convertDate("2019-01-07T11:29:23.9840087");
        new GenericCalculation().fetchTenMinuteAggregatedData("2017-03-13", "WindSpeed");
    }
    
    public ArrayList<HashMap<String,String>> fetchAlarmCategory(){
        ResultSet rsObj = null;
        java.sql.Connection
        connObj = null;
        PreparedStatement pstmtObj = null;
        DBConnection jdbcObj = new DBConnection();
        ArrayList<HashMap<String,String>> hmapList=new ArrayList<HashMap<String,String>>();
        try{
            DataSource dataSource = jdbcObj.setUpPool();
            connObj = dataSource.getConnection();
            Statement statement = connObj.createStatement();
            
            rsObj = statement.executeQuery("SELECT EVENTCODE,FIRSTFAULT,SITEID,CATEGORY FROM event_master");
            
            
            while (rsObj.next()) {
                String eventCode=(String)rsObj.getObject("EVENTCODE");
                String firstFault=(String)rsObj.getObject("FIRSTFAULT");
                String siteId=(String)rsObj.getObject("SITEID");
                String category=(String)rsObj.getObject("CATEGORY");
                
                if(category != null && siteId !=null && firstFault != null && eventCode != null){
                
                    HashMap<String,String> resultMap=new HashMap<String,String>();
                    resultMap.put("eventCode",eventCode);
                    resultMap.put("firstFault",firstFault);
                    resultMap.put("siteId",siteId);
                    resultMap.put("category",category);

                    hmapList.add(resultMap);
                }
                
            }
                
            System.out.println("Hash Map List:"+hmapList);

            return hmapList;
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
    
    public static long convertDate(String dateInString)
    {
        try{
            
            
            LocalDateTime localDateTime = LocalDateTime.parse(dateInString);
            
            Date out = Date.from(localDateTime.atZone(ZoneId.of("UTC")).toInstant());
           
            //long reverseTS=Long.MAX_VALUE-out.getTime();
            
            System.out.println("Timestamp: "+out.getTime()/1000);
            System.out.println("");
            
            /*DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-mm-dd HH:mm:ss");
            DateTime date = formatter.parseDateTime(dateInString);
            
            System.out.println("Date :"+date);*/
            
            return out.getTime()/1000;
        }
        catch(Exception e){
            e.printStackTrace();
            long temp=-1;
            return temp;
        }
    }
}
