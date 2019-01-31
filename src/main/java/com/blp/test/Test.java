/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.blp.test;

import com.blp.dbconnection.DBConnection;
import com.blp.dbconnection.DBUtils;
import com.blp.ingesttenminagg.InsertTenMinuteAggregate;
import com.blp.kpicalculation.KPICalculation;
import static com.blp.kpicalculation.KPICalculation.instanceId;
import static com.blp.kpicalculation.KPICalculation.projectId;
import static com.blp.kpicalculation.KPICalculation.tableId;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.sql.DataSource;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 *
 * @author siva.kumar
 */
public class Test {
    public String genericFunctionForKPI(String startDateInString,String endDateInString){
        try{
            
            byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("tag");
            byte[] COLUMN_NAME = Bytes.toBytes("AC_Calc");
            
            System.out.println("Start Date: " + startDateInString);
            long startDate = KPICalculation.convertDate(startDateInString);
            System.out.println("End Date: " + endDateInString);
            long endDate = KPICalculation.convertDate(endDateInString);
            
            try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
            
                String rowKey = Long.toString(startDate);
                String stopValue = Long.toString(endDate);
                
                Scan scan = new Scan().withStartRow(Bytes.toBytes(rowKey), true).withStopRow(Bytes.toBytes(stopValue), true);
                //Scan scan = new Scan();
                
                Table table = connection.getTable(TableName.valueOf(tableId));

                ResultScanner scanner = table.getScanner(scan);
                
                Map<String, List<HashMap<String,Long>>> segList= new HashMap<>();
                HashMap<String,String> assetSiteIdMap=new HashMap<>();
                HashMap<String,String> assetIdassetTypeMap=new HashMap<>();
                Map<String, List<HashMap<String,Long>>> afcMapList= new HashMap<>();
                ArrayList<HashMap<String,String>> ffcMapList= new ArrayList<>();
                
                System.out.println("Scanner Starts .........");
                
                for (Result row : scanner) {
                    byte[] valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
                    if(valueBytes != null){
                        if(segList.get(Bytes.toString(row.getRow()).split("#")[2]) != null){
                            //System.out.println("Seg List Inner Loop:"+segList.get(Bytes.toString(row.getRow()).split("#")[1]));
                            List<HashMap<String,Long>> valueList= segList.get(Bytes.toString(row.getRow()).split("#")[2]);
                            HashMap<String,Long> tempMap=new HashMap<String,Long>();
                            tempMap.put("timeStamp",Long.parseLong(Bytes.toString(row.getRow()).split("#")[0]));
                            int floatValue=(int)Float.parseFloat(Bytes.toString(valueBytes));
                            tempMap.put("alarmCode",Integer.toUnsignedLong(floatValue));
                            valueList.add(tempMap);
                            segList.put(Bytes.toString(row.getRow()).split("#")[2],valueList);
                            assetSiteIdMap.put(Bytes.toString(row.getRow()).split("#")[2],Bytes.toString(row.getRow()).split("#")[1]);
                            assetIdassetTypeMap.put(Bytes.toString(row.getRow()).split("#")[2],Bytes.toString(row.getRow()).split("#")[3]);
                        }
                        else{
                            List<HashMap<String,Long>> valueList=new ArrayList<>();
                            HashMap<String,Long> tempMap=new HashMap<>();
                            tempMap.put("timeStamp",Long.parseLong(Bytes.toString(row.getRow()).split("#")[0]));
                            int floatValue=(int)Float.parseFloat(Bytes.toString(valueBytes));
                            tempMap.put("alarmCode",Integer.toUnsignedLong(floatValue));
                            valueList.add(tempMap);
                            segList.put(Bytes.toString(row.getRow()).split("#")[2],valueList);
                            assetSiteIdMap.put(Bytes.toString(row.getRow()).split("#")[2],Bytes.toString(row.getRow()).split("#")[1]);
                            assetIdassetTypeMap.put(Bytes.toString(row.getRow()).split("#")[2],Bytes.toString(row.getRow()).split("#")[3]);
                        }
                    }
                }

                //System.out.println(segList);
                
                for(Map.Entry<String,List<HashMap<String,Long>>> entry : segList.entrySet()){
                    
                    //new Test().sortTimeStamp(entry.getValue());
                    System.out.println("Asset Id:"+entry.getKey());
                    
                    String moduleName=assetIdassetTypeMap.get(entry.getKey());
                    
                    ArrayList<HashMap<String,String>> alarmCodeMapList=new DBUtils().fetchalarmcode(moduleName);
                    
                    if(!alarmCodeMapList.isEmpty()){
                    
                        for(HashMap<String,Long> dataMap:entry.getValue()){
                            for(HashMap<String,String> alarmCode:alarmCodeMapList){
                                long tempAlarmCode=dataMap.get("alarmCode");
                                int alarmFromBT=(int)tempAlarmCode-6441;
                                int alarmDataFromSQl=Integer.parseInt(alarmCode.get("alarmCode"));
                                if(alarmFromBT == alarmDataFromSQl){
                                    //System.out.println("Hereeeeeeee");
                                    dataMap.put("faultState", Long.parseLong(alarmCode.get("faultState")));
                                }
                            }
                        }

                        // Remove all the warning state codes  

                        for(HashMap<String,Long> dataMap:entry.getValue()){
                            long faultState=dataMap.get("faultState");
                            if((int)faultState == 1){
                                dataMap.clear();
                            }
                        }

                        //System.out.println("AssetId:"+entry.getKey()+" : "+entry.getValue());

                        for(HashMap<String,Long> dataMap:entry.getValue()){
                            if(!dataMap.isEmpty()){
                                System.out.println("DataMap:"+dataMap);
                                if(afcMapList.isEmpty()){
                                    List<HashMap<String,Long>> valueList=new ArrayList<>();
                                    valueList.add(dataMap);
                                    afcMapList.put(entry.getKey(),valueList);
                                }
                                else{
                                    HashMap<String,Long> afcMap=new HashMap<>();
                                    List<HashMap<String,Long>> valueList=new ArrayList<>();
                                    valueList=afcMapList.get(entry.getKey());
                                    if(valueList == null || valueList.isEmpty()){
                                        valueList=new ArrayList<>();
                                        valueList.add(dataMap);
                                        afcMapList.put(entry.getKey(),valueList);
                                    }
                                    else{
                                        int size=valueList.size();
                                        afcMap=valueList.get(size-1);
                                        long previousfaultCode=afcMap.get("faultState");
                                        long curentfaultCode=dataMap.get("faultState");
                                        if((int)previousfaultCode != (int)curentfaultCode){
                                            valueList.add(dataMap);
                                            afcMapList.put(entry.getKey(),valueList);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                
                System.out.println("AFC List:  "+afcMapList);
                
                for(Map.Entry<String,List<HashMap<String,Long>>> entry : afcMapList.entrySet()){
                    boolean flag=false;
                    for(HashMap<String,Long> dataMap:entry.getValue()){
                        long faultCode=dataMap.get("faultState");
                        if((int)faultCode == 2){
                            flag=true;
                            HashMap<String,String> tempMap=new HashMap<>();
                            tempMap.put("startfaultState",Long.toString(faultCode));
                            tempMap.put("startTimeStamp",Long.toString(dataMap.get("timeStamp")));
                            tempMap.put("alarmCode", Long.toString(dataMap.get("alarmCode")));
                            tempMap.put("assetId",entry.getKey());
                            tempMap.put("siteId",assetSiteIdMap.get(entry.getKey()));
                            tempMap.put("assetType",assetIdassetTypeMap.get(entry.getKey()));
                            tempMap.put("flagState","two");
                            ffcMapList.add(tempMap);
                        }
                        if((int)faultCode == 0){
                            if(flag){
                                for(HashMap<String,String> resultMap:ffcMapList){
                                    if(resultMap.get("assetId").equalsIgnoreCase(entry.getKey()) && 
                                            resultMap.get("flagState").equalsIgnoreCase("two")){
                                        resultMap.put("endfaultState",Long.toString(faultCode));
                                        resultMap.put("endTimeStamp",Long.toString(dataMap.get("timeStamp")));
                                        resultMap.put("flagState","one");
                                    }
                                }
                            }
                                
                        }
                    }
                    System.out.println("FFC List First Set:"+ffcMapList);
                    new Test().calculateSecondFFCLogic(ffcMapList,startDate,endDate);
                }
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
        try{
            /*long updateStartTime=Long.parseLong("1556799143000");
            long updatedEndTime=Long.parseLong("1556801963000");
            
            DateTime updateStartDate=new DateTime(updateStartTime);
            DateTime updateEndDate=new DateTime(updatedEndTime);
            
            System.out.println("Start Date : "+updateStartDate+" : "+updateEndDate);
            
            
            boolean flag=false;
            if(updateStartDate.getSecondOfMinute()!= 0){
                flag=true;
                updateStartDate=updateStartDate.plusMinutes(10);
                
            }
            if(updateStartDate.getMinuteOfHour() % 10 != 0){
                updateStartDate=updateStartDate.minusMinutes(updateStartDate.getMinuteOfHour() % 10);
                if(!flag){
                    updateStartDate=updateStartDate.plusMinutes(10);
                }
            }
            flag=false;
            if(updateEndDate.getSecondOfMinute()!= 0){
                if(updateEndDate.getMinuteOfHour() % 10 != 0){
                    flag=true;
                    updateEndDate=updateEndDate.minusMinutes(updateEndDate.getMinuteOfHour() % 10);
                }
            }
            if(updateEndDate.getMinuteOfHour() % 10 == 0){
                if(!flag){
                    updateEndDate=updateEndDate.minusMinutes(10);
                }
            }
            if(updateEndDate.getMinuteOfHour() % 10 != 0){
                    updateEndDate=updateEndDate.minusMinutes(updateEndDate.getMinuteOfHour() % 10);
                }
            
            updateStartDate=updateStartDate.withSecondOfMinute(0);
            updateEndDate=updateEndDate.withSecondOfMinute(0);
            System.out.println("Start Date : "+updateStartDate+" : "+updateEndDate);*/
            
        //new Test().genericFunctionForKPI();
        //new Test().fetchAlarmCategory();
        //Test.convertDate("2019-01-07T11:29:23.9840087");
        new Test().genericFunctionForKPI("2017-12-02T00:00:00", "2017-12-03T00:00:00");
        //new GenericCalculation().fetchTenMinuteAggregatedData("2017-03-13", "WindSpeed");
        }
        catch(Exception e){
            e.printStackTrace();
        }
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
                
            //System.out.println("Hash Map List:"+hmapList);

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
    public String sortTimeStamp(List<HashMap<String,String>> timestamp){
        try{
            ArrayList<TreeMap<String,Long>> tempMapList=new ArrayList<>();
            for(HashMap<String,String> dataMap:timestamp){
                TreeMap<String,Long> tempMap=new TreeMap<>();
                tempMap.put("timeStamp",Long.parseLong(dataMap.get("timeStamp")));
                int floatValue=(int)Float.parseFloat(dataMap.get("alarmCode"));
                tempMap.put("alarmCode",Integer.toUnsignedLong(floatValue));
                tempMapList.add(tempMap);
            }
            System.out.println(tempMapList);
            
            for(TreeMap<String,Long> tempMap:tempMapList){
                
            }
            
            return null;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }
    public String calculateSecondFFCLogic(ArrayList<HashMap<String,String>> ffcList,long startDate,long endDate) {
        try{
            ArrayList<HashMap<String,String>> secondSetFFCList=new ArrayList<>();
            
            for(HashMap<String,String> ffcMap:ffcList){
                //String moduleName=new DBUtils().fetchModuleName(ffcMap.get("assetId"));
                String moduleName=ffcMap.get("assetType");
                JSONParser parser=new JSONParser();
                Object obj=parser.parse(new FileReader("prop.json"));
                JSONObject jsonObject = (JSONObject) obj;
                
                Set<String> keys=jsonObject.keySet();

                for(String key:keys){
                    if(key.equalsIgnoreCase(moduleName.toLowerCase())){
                        JSONObject propertyObject=(JSONObject)jsonObject.get(key);
                        String iterativeCount=(String)propertyObject.get("active_count");
                        String faultValue=(String)propertyObject.get("fault_value");
                        String comparision=(String)propertyObject.get("comparision");
                        
                        // Pass the time stamp of start to end and get the 10 min agg active power of that time stamp.
                        // Compare that result with the property file comparision value
                        
                        String startTimeStamp=ffcMap.get("startTimeStamp");
                        String endTimeStamp=ffcMap.get("endTimeStamp");
                        
                        DateTime updateStartDate=new DateTime(Long.parseLong(startTimeStamp)*1000);
                        DateTime updateEndDate=new DateTime(Long.parseLong(endTimeStamp)*1000);

                        System.out.println("Start Date : "+updateStartDate+" : "+updateEndDate);


                        boolean flag=false;
                        if(updateStartDate.getSecondOfMinute()!= 0){
                            flag=true;
                            updateStartDate=updateStartDate.plusMinutes(10);

                        }
                        if(updateStartDate.getMinuteOfHour() % 10 != 0){
                            updateStartDate=updateStartDate.minusMinutes(updateStartDate.getMinuteOfHour() % 10);
                            if(!flag){
                                updateStartDate=updateStartDate.plusMinutes(10);
                            }
                        }
                        flag=false;
                        if(updateEndDate.getSecondOfMinute()!= 0){
                            if(updateEndDate.getMinuteOfHour() % 10 != 0){
                                flag=true;
                                updateEndDate=updateEndDate.minusMinutes(updateEndDate.getMinuteOfHour() % 10);
                            }
                        }
                        if(updateEndDate.getMinuteOfHour() % 10 == 0){
                            if(!flag){
                                updateEndDate=updateEndDate.minusMinutes(10);
                            }
                        }
                        if(updateEndDate.getMinuteOfHour() % 10 != 0){
                                updateEndDate=updateEndDate.minusMinutes(updateEndDate.getMinuteOfHour() % 10);
                            }

                        updateStartDate=updateStartDate.withSecondOfMinute(0);
                        updateEndDate=updateEndDate.withSecondOfMinute(0);
                        System.out.println("Start Date : "+updateStartDate+" : "+updateEndDate);
                        
                        String startDateUpdated=new Test().convertDateTimeToString(updateStartDate);
                        String endDateUpdated=new Test().convertDateTimeToString(updateEndDate);
                        
                        secondSetFFCList=new DBUtils().fecthActivePowerForFFCCalculation(startDateUpdated,endDateUpdated,ffcMap.get("assetId"),comparision,iterativeCount,faultValue);
                        
                        System.out.println("second ffc list"+secondSetFFCList);
                        
                        for(HashMap<String,String> secondSetFFCMap:secondSetFFCList){
                            if(secondSetFFCMap.get("assetId").equalsIgnoreCase(ffcMap.get("assetId"))){
                                System.out.println("Fault Compared");
                                String secondSetEndTimeStamp=secondSetFFCMap.get("endTimeStamp");
                                SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                Date secondSetEndDate=sdf.parse(secondSetEndTimeStamp);
                                Date firstSetEndDate=new Date(Long.parseLong(ffcMap.get("endTimeStamp"))*1000);
                                if(secondSetEndDate.compareTo(firstSetEndDate) < 0){
                                    ffcMap.put("endTimeStamp", Long.toString(secondSetEndDate.getTime()));
                                    ffcMap.put("endfaultState","0");
                                    ffcMap.put("flagState","one");
                                }
                            }
                        }
                    }
                }
            }
            
            System.out.println("FFC List Second Set:"+ffcList);
            
            new Test().calculateSOKforTenMinuteAgg(ffcList,startDate,endDate);
            
            return null;
        }
        catch(Exception e)
        {
            e.printStackTrace();
            return null;
        }
    } 
    
    public String convertDateTimeToString(DateTime dateTime){
        try{
            DecimalFormat df = new DecimalFormat("00");
            
            String startYear = Integer.toString(dateTime.getYear());
            String startMonth = df.format(dateTime.getMonthOfYear());
            String startDay = df.format(dateTime.getDayOfMonth());
            String startHour = df.format(dateTime.getHourOfDay());
            String startMinute = df.format(dateTime.getMinuteOfHour());
            String startSeconds = df.format(dateTime.getSecondOfMinute());

            
            String tempDateInString = startYear + "-" + startMonth + "-" + startDay + " " + startHour + ":" + startMinute + ":" + startSeconds;
            
            
            return tempDateInString;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }
    
    public String calculateSOKforTenMinuteAgg(ArrayList<HashMap<String,String>> ffcList,long startDate,long endDate){
        try{
            
            ArrayList<HashMap<String,String>> resultList=new ArrayList<>();
            
            for(HashMap<String,String> ffcMap : ffcList){
                
                DateTime updateStartDate=new DateTime(startDate*1000);
                DateTime updateEndDate=new DateTime(endDate*1000);
                
                DateTime startFaultTime=new DateTime(Long.parseLong(ffcMap.get("startTimeStamp"))*1000);
                DateTime endFaultTime=new DateTime(Long.parseLong(ffcMap.get("endTimeStamp"))*1000);
                    
                while (updateStartDate.compareTo(updateEndDate) != 0) {

                    DateTime tempDate = updateStartDate.plusMinutes(10);

                    if(updateStartDate.compareTo(startFaultTime) < 0 && tempDate.compareTo(startFaultTime) >0){
                        System.out.println("Start Time:"+updateStartDate);

                        if(tempDate.compareTo(endFaultTime) > 0){

                            long diffInSeconds = (endFaultTime.getMillis()- startFaultTime.getMillis())/1000;
                            HashMap<String,String> dateMap=new HashMap<>();

                            dateMap.put("startDate",Long.toString(updateStartDate.getMillis()/1000));
                            dateMap.put("endDate",Long.toString(tempDate.getMillis()/1000));
                            dateMap.put("uptime", Long.toString(600-diffInSeconds));
                            dateMap.put("assetId",ffcMap.get("assetId"));
                            dateMap.put("tagName","SOK");
                            dateMap.put("siteId",ffcMap.get("siteId"));
                            
                            resultList.add(dateMap);
                            
                            updateStartDate=updateStartDate.plusMinutes(10);
                        }
                        else{

                            long diffInSeconds = (endFaultTime.getMillis()- startFaultTime.getMillis())/1000;
                            System.out.println("Diff In seconds"+diffInSeconds);
                            long secondsDiffOfStartTimeToStartFault=(startFaultTime.getMillis()-updateStartDate.getMillis())/1000;
                            System.out.println("Diff In start"+secondsDiffOfStartTimeToStartFault);
                            //long secondsDiffOfStartTimeToStartFault=(startFaultTime.getMillis()-updateStartDate.getMillis())/1000;
                            HashMap<String,String> dateMap=new HashMap<>();

                            dateMap.put("startDate",Long.toString(updateStartDate.getMillis()/1000));
                            dateMap.put("endDate",Long.toString(tempDate.getMillis()/1000));
                            dateMap.put("uptime", Long.toString(secondsDiffOfStartTimeToStartFault));
                            dateMap.put("assetId",ffcMap.get("assetId"));
                            dateMap.put("tagName","SOK");
                            dateMap.put("siteId",ffcMap.get("siteId"));
                            resultList.add(dateMap);
                            
                            updateStartDate=updateStartDate.plusMinutes(10);

                            diffInSeconds=diffInSeconds-(600-secondsDiffOfStartTimeToStartFault);

                            if(diffInSeconds > 600){
                                while(diffInSeconds != 0){
                                    if(diffInSeconds > 600){
                                        diffInSeconds=diffInSeconds-600;

                                        DateTime tempStart=updateStartDate.plusMinutes(10);
                                        DateTime tempEnd=tempStart.plusMinutes(10);

                                        HashMap<String,String> dateMapOne=new HashMap<>();

                                        dateMapOne.put("startDate",Long.toString(tempStart.getMillis()/1000));
                                        dateMapOne.put("endDate",Long.toString(tempEnd.getMillis()/1000));
                                        dateMapOne.put("uptime", "0");
                                        dateMapOne.put("assetId",ffcMap.get("assetId"));
                                        dateMapOne.put("tagName","SOK");
                                        dateMapOne.put("siteId",ffcMap.get("siteId"));
                                        
                                        updateStartDate=updateStartDate.plusMinutes(10);
                                
                                        resultList.add(dateMapOne);
                                    }
                                    else{
                                        DateTime tempStart=updateStartDate.plusMinutes(10);
                                        DateTime tempEnd=tempStart.plusMinutes(10);

                                        HashMap<String,String> dateMapTwo=new HashMap<>();

                                        dateMapTwo.put("startDate",Long.toString(tempStart.getMillis()/1000));
                                        dateMapTwo.put("endDate",Long.toString(tempEnd.getMillis()/1000));
                                        dateMapTwo.put("uptime", Long.toString(600-diffInSeconds));
                                        dateMapTwo.put("assetId",ffcMap.get("assetId"));
                                        dateMapTwo.put("tagName","SOK");
                                        dateMapTwo.put("siteId",ffcMap.get("siteId"));
                                        resultList.add(dateMapTwo);
                                        
                                        updateStartDate=updateStartDate.plusMinutes(10);
                                
                                        diffInSeconds=0;
                                    }
                                }
                            }
                            else{
                                DateTime tempStart=updateStartDate.plusMinutes(10);
                                DateTime tempEnd=tempStart.plusMinutes(10);

                                HashMap<String,String> dateMapTwo=new HashMap<>();

                                dateMapTwo.put("startDate",Long.toString(tempStart.getMillis()/1000));
                                dateMapTwo.put("endDate",Long.toString(tempEnd.getMillis()/1000));
                                dateMapTwo.put("uptime", Long.toString(600-diffInSeconds));
                                dateMapTwo.put("assetId",ffcMap.get("assetId"));
                                dateMapTwo.put("tagName","SOK");
                                dateMapTwo.put("siteId",ffcMap.get("siteId"));
                                resultList.add(dateMapTwo);
                                
                                updateStartDate=updateStartDate.plusMinutes(10);
                                
                                diffInSeconds=0;
                            }
                            break;
                        }
                    }
                    else{
                        HashMap<String,String> dateMap=new HashMap<>();

                        dateMap.put("startDate",Long.toString(updateStartDate.getMillis()/1000));
                        dateMap.put("endDate",Long.toString(tempDate.getMillis()/1000));
                        dateMap.put("uptime", "600");
                        dateMap.put("assetId",ffcMap.get("assetId"));
                        dateMap.put("tagName","SOK");
                        dateMap.put("siteId",ffcMap.get("siteId"));
                        
                        resultList.add(dateMap);
                        
                        updateStartDate=updateStartDate.plusMinutes(10);
                    }
                    
                }
            }

            System.out.println("Result List Ten Minute Aggregate: "+resultList.size());
            System.out.println("Result List Ten Minute Aggregate: "+resultList);
            
            //new InsertTenMinuteAggregate().insertTenMinuteAggregateForSOK(resultList);
            
            //Loop all 10 min date range -> if downtime lies then minus it from 600 
            //else add 600 to this ten minute window
            
            
            
            return null;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }
    
    
}
