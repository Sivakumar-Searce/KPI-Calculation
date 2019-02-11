/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.blp.kpicalculation;

import com.blp.dbconnection.DBUtils;
import com.blp.ingesttenminagg.InsertTenMinuteAggregate;
import com.blp.test.Test;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author Sivakumar
 */
public class KPICalculation {

    public static String projectId = "platform-dev-blp";  // my-gcp-project-id
    public static String instanceId = "blp-dev-bt"; // my-bigtable-instance-id
    public static String tableId = "historical_wind_alarmdata";
    //public static String tableId = "validate_amb"; //for KPI Calculation
    
    public static void callKPIMethods(String KPIname,String fromDate,String toDate,String siteId){
        try{
            if(KPIname.equalsIgnoreCase("GenrationKPI")){
                new KPICalculation().generationKPI(fromDate,siteId);
            }
            else if(KPIname.equalsIgnoreCase("ActualProduction")){
                KPICalculation.iterateDaysForKPICall(KPIname, fromDate, toDate, siteId);
            }
            else if(KPIname.equalsIgnoreCase("WindSpeed")){
                KPICalculation.iterateDaysForKPICall(KPIname, fromDate, toDate, siteId);
            }
            else if(KPIname.equalsIgnoreCase("PotentialProduction")){
                KPICalculation.iterateDaysForKPICall(KPIname, fromDate, toDate, siteId);
            }
            else if(KPIname.equalsIgnoreCase("SOK")){
                new Test().sokCalculationKPI(fromDate,toDate,siteId);
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
    
    public static void iterateDaysForKPICall(String KPIname,String fromDate,String toDate,String siteId){
        try{
            LocalDateTime startlocalDateTime = LocalDateTime.parse(fromDate);
            Instant startinstant= new Instant(startlocalDateTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
            DateTime startdate=new DateTime(startinstant);
            
            LocalDateTime tolocalDateTime = LocalDateTime.parse(toDate);
            Instant endinstant= new Instant(tolocalDateTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
            DateTime enddate=new DateTime(endinstant);
            
            while (startdate.compareTo(enddate) != 0) {
                DateTime tempDate = startdate;
                startdate = startdate.plusMinutes(10);  
                
                DateTimeFormatter startformatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                
                if(KPIname.equalsIgnoreCase("ActualProduction")){
                    new KPICalculation().actualProductionKPICalculation(startformatter.print(tempDate).replace(" ", "T"),startformatter.print(startdate).replace(" ", "T"));
                }
                else if(KPIname.equalsIgnoreCase("WindSpeed")){
                    new KPICalculation().windSpeedKPICalculation(startformatter.print(tempDate).replace(" ", "T"),startformatter.print(startdate).replace(" ", "T"));
                }
                else if(KPIname.equalsIgnoreCase("PotentialProductionWhenAvailable")){
                    new KPICalculation().potentialProductionWhenAvailableKPICalculation(startformatter.print(tempDate).replace(" ", "T"),startformatter.print(startdate).replace(" ", "T"));
                }
                else if(KPIname.equalsIgnoreCase("PotentialProduction")){
                    new KPICalculation().potentialProductionKPICalculation(startformatter.print(tempDate).replace(" ", "T"),startformatter.print(startdate).replace(" ", "T"));
                }
            }
            
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
    
    /*
    This is test Function for testing purpose
    */
    public String genericFunctionForKPI(String startDateInString, String endDateInString, String assetId, String eventName, String familyGroup) {
        try {
            long startDate = KPICalculation.convertDate(startDateInString);
            long endDate = KPICalculation.convertDate(endDateInString);

            byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(familyGroup);
            byte[] COLUMN_NAME = Bytes.toBytes(eventName);

            try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

                String rowKey = Long.toString(Long.MAX_VALUE - endDate);
                String stopValue = Long.toString(Long.MAX_VALUE - startDate);

                Scan scan = new Scan().withStartRow(Bytes.toBytes(rowKey), true).withStopRow(Bytes.toBytes(stopValue), true);

                Table table = connection.getTable(TableName.valueOf(tableId));

                ResultScanner scanner = table.getScanner(scan);

                Map<String, List<Double>> segList = new HashMap<String, List<Double>>();

                for (Result row : scanner) {
                    byte[] valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
                    if (valueBytes != null) {
                        if (segList.get(Bytes.toString(row.getRow()).split("#")[1]) != null) {
                            //System.out.println("Seg List Inner Loop:"+segList.get(Bytes.toString(row.getRow()).split("#")[1]));
                            List<Double> valueList = segList.get(Bytes.toString(row.getRow()).split("#")[1]);
                            valueList.add(Double.valueOf(Bytes.toString(valueBytes)));
                            segList.put(Bytes.toString(row.getRow()).split("#")[1], valueList);
                        } else {
                            List<Double> valueList = new ArrayList<>();
                            valueList.add(Double.valueOf(Bytes.toString(valueBytes)));
                            segList.put(Bytes.toString(row.getRow()).split("#")[1], valueList);
                        }
                    }
                }

                //System.out.println(segList);
                ArrayList<HashMap<String, String>> listOfResults = new ArrayList<>();

                for (Map.Entry<String, List<Double>> entry : segList.entrySet()) {

                    HashMap<String, String> resultMap = new HashMap<>();

                    double summationValue = KPICalculation.sum(entry.getValue());
                    double averageSum = summationValue / entry.getValue().size();

                    resultMap.put("assetId", entry.getKey());
                    resultMap.put("averageValue", Double.toString(averageSum));
                    resultMap.put("tagName", Bytes.toString(COLUMN_NAME));

                    listOfResults.add(resultMap);
                }

                System.out.println(listOfResults);
            } catch (Exception e) {
                e.printStackTrace();
            }

            return "success";
        } catch (Exception e) {
            return null;
        }
    }
    /*
    Converting Date given in string standard date time format and returns an dateTime object
    */
    public static long convertDate(String dateInString) {
        try {

            LocalDateTime localDateTime = LocalDateTime.parse(dateInString);
            Instant instant= new Instant(localDateTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
            DateTime date=new DateTime(instant);
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            System.out.println("Formatted Date :"+formatter.print(date));
            
            /*Date out = Date.from(localDateTime.atZone(ZoneId.of("UTC")).toInstant());

            //long reverseTS=Long.MAX_VALUE-out.getTime();
            System.out.println("Timestamp: " + out.getTime() / 1000);
            System.out.println("");*/

            /*DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-mm-dd HH:mm:ss");
            DateTime date = formatter.parseDateTime(dateInString);
            
            System.out.println("Date :"+date);*/
            return date.getMillis()/1000;
        } catch (Exception e) {
            e.printStackTrace();
            long temp = -1;
            return temp;
        }
    }
    
    /*
    Main Function 
    1) Assign the startDate and endDate 
    2) Does 10 min aggregation and stores in DB
    */
    
    public static void main(String args[]) {

        DateTime startDate = new DateTime(2017, 03, 13, 00, 00, 00);

        DateTime endDate = new DateTime(2017, 03, 15, 00, 00, 00);

        DecimalFormat df = new DecimalFormat("00");
        int count = 0;

        while (startDate.compareTo(endDate) != 0) {
            DateTime tempDate = startDate;
            startDate = startDate.plusMinutes(10);

            String startYear = Integer.toString(tempDate.getYear());
            String startMonth = df.format(tempDate.getMonthOfYear());
            String startDay = df.format(tempDate.getDayOfMonth());
            String startHour = df.format(tempDate.getHourOfDay());
            String startMinute = df.format(tempDate.getMinuteOfHour());
            String startSeconds = df.format(tempDate.getSecondOfMinute());

            String endYear = Integer.toString(startDate.getYear());
            String endMonth = df.format(startDate.getMonthOfYear());
            String endDay = df.format(startDate.getDayOfMonth());
            String endHour = df.format(startDate.getHourOfDay());
            String endMinute = df.format(startDate.getMinuteOfHour());
            String endSeconds = df.format(startDate.getSecondOfMinute());

            String tempDateInString = startYear + "-" + startMonth + "-" + startDay + "T" + startHour + ":" + startMinute + ":" + startSeconds;
            String endDateInString = endYear + "-" + endMonth + "-" + endDay + "T" + endHour + ":" + endMinute + ":" + endSeconds;

            //System.out.println("Generation KPI");
            //new KPICalculation().generationKPI(tempDateInString, endDateInString);
            //System.out.println("Actual Productioin KPI");
            new KPICalculation().actualProductionKPICalculation(tempDateInString, endDateInString);
            //System.out.println("Potential Productioin KPI");
            //new KPICalculation().potentialProductionKPICalculation(tempDateInString, endDateInString);
            //System.out.println("Wind Speed KPI");
            //new KPICalculation().windSpeedKPICalculation(tempDateInString, endDateInString);

            count++;
        }
        System.out.println("No. of Date range iterated:" + count);
        
        //new KPICalculation().generationKPI("2018-05-05T00:00:00");
    }
    
    /*
    Calculates the Daily Genration KPI
    */

    public String generationKPI(String dateInString,String siteId) {
        try {
            byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("tag");
            byte[] COLUMN_NAME = Bytes.toBytes("TotalProduction_Raw");

            System.out.println("Data Proccesing Starts");
            
            LocalDateTime localDateTime = LocalDateTime.parse(dateInString);

            Date startDate = Date.from(localDateTime.atZone(ZoneId.of("UTC")).toInstant());
            
            startDate.setHours(00);
            startDate.setMinutes(00);
            //Adding 10 minute
            Date endDate=new Date(startDate.getTime()+(10*60000));
            
            long startDateInSeconds = startDate.getTime();
            long endDateInSeconds = endDate.getTime();
            
            System.out.println(startDate+":"+endDate);
            
            ArrayList<HashMap<String, String>> resultListMap = new ArrayList<HashMap<String, String>>();

            try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

                String rowKey = Long.toString(startDateInSeconds);
                String stopValue = Long.toString(endDateInSeconds);

                Scan scan = new Scan().withStartRow(Bytes.toBytes(rowKey), true).withStopRow(Bytes.toBytes(stopValue), true);

                //Scan scan = new Scan();
                Table table = connection.getTable(TableName.valueOf(tableId));

                ResultScanner scanner = table.getScanner(scan);

                boolean isColumnExists = false;

                for (Result row : scanner) {
                    byte[] valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
                    if (valueBytes != null) {
                        isColumnExists = true;
                        //System.out.println("value:"+Bytes.toString(valueBytes));
                    }
                }
                if (isColumnExists) {
                    
                    System.out.println("TotalProduction_Raw");
                    Date startlastTenMinuteDate=startDate;
                    
                    startlastTenMinuteDate.setHours(23);
                    startlastTenMinuteDate.setMinutes(50);
                    
                    Date endlastTenMinuteDate=new Date(startlastTenMinuteDate.getTime()+(10*60000));
                    
                    long startTenMinuteInSeconds= startlastTenMinuteDate.getTime()/1000;
                    long endTenMinuteInSeconds= endlastTenMinuteDate.getTime()/1000;
                    
                    String startTenMinuterowKey = Long.toString(startTenMinuteInSeconds);
                    String endTenMinuterowKey = Long.toString(endTenMinuteInSeconds);

                    System.out.println(startlastTenMinuteDate+":"+endlastTenMinuteDate);
                    
                    Scan scanLastTenMinute = new Scan().withStartRow(Bytes.toBytes(startTenMinuterowKey), true).withStopRow(Bytes.toBytes(endTenMinuterowKey), true);
                    
                    ResultScanner scannerLastTenMinute = table.getScanner(scanLastTenMinute);
                    
                    boolean isLastTenMinute=false;
                    for (Result row : scannerLastTenMinute) {
                        byte[] valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
                        if (valueBytes != null) {
                            isLastTenMinute = true;
                            //System.out.println("value:"+Bytes.toString(valueBytes));
                        }
                    }
                    
                    if(isLastTenMinute){
                        ArrayList<HashMap<String,String>> minimumResultsList = new GenericCalculation().processScanedDataForGeneration("TotalProduction_Raw", table.getScanner(scan), startDateInSeconds, endDateInSeconds,"min");
                        //new InsertTenMinuteAggregate().insertTenMinuteAggregate(resultListMap);

                        ArrayList<HashMap<String,String>> maximumResultsList = new GenericCalculation().processScanedDataForGeneration("TotalProduction_Raw", table.getScanner(scanLastTenMinute), startTenMinuteInSeconds, endTenMinuteInSeconds,"max");
                        //System.out.println("");
                        for(HashMap<String,String> maxMap:maximumResultsList){
                            for(HashMap<String,String> minMap:minimumResultsList){
                                if(maxMap.get("assetId").equalsIgnoreCase(minMap.get("assetId"))){
                                    HashMap<String,String> resultMap=new HashMap<>();
                                    resultMap.put("assetId",maxMap.get("assetId"));
                                    resultMap.put("siteId",maxMap.get("siteId"));
                                    double generationValue=Double.parseDouble(maxMap.get("max"))-Double.parseDouble(minMap.get("min"));
                                    resultMap.put("generationValue",Double.toString(generationValue));
                                    SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
                                    resultMap.put("date",sdf.format(startDate));
                                    resultListMap.add(resultMap);
                                }
                            }
                        }

                        System.out.println(resultListMap);
                        new InsertTenMinuteAggregate().insertDailyGenerationAggregate(resultListMap);
                        
                        return "success";
                    }
                    else{
                        isColumnExists=false;
                    }
                } 
                if(!isColumnExists) {
                    for (Result row : table.getScanner(scan)) {
                        byte[] valueBytes = row.getValue(COLUMN_FAMILY_NAME, Bytes.toBytes("TotalProduction"));
                        if (valueBytes != null) {
                            isColumnExists = true;
                            //System.out.println("value:"+Bytes.toString(valueBytes));
                        }
                    }
                }

                if (isColumnExists) {
                    System.out.println("TotalProduction");
                    Date startlastTenMinuteDate=startDate;
                    
                    startlastTenMinuteDate.setHours(23);
                    startlastTenMinuteDate.setMinutes(50);
                    
                    Date endlastTenMinuteDate=new Date(startlastTenMinuteDate.getTime()+(10*60000));
                    
                    long startTenMinuteInSeconds= startlastTenMinuteDate.getTime()/1000;
                    long endTenMinuteInSeconds= endlastTenMinuteDate.getTime()/1000;
                    
                    String startTenMinuterowKey = Long.toString(startTenMinuteInSeconds);
                    String endTenMinuterowKey = Long.toString(endTenMinuteInSeconds);

                    
                    Scan scanLastTenMinute = new Scan().withStartRow(Bytes.toBytes(startTenMinuterowKey), true).withStopRow(Bytes.toBytes(endTenMinuterowKey), true);
                    
                    ResultScanner scannerLastTenMinute = table.getScanner(scanLastTenMinute);
                    
                    boolean isLastTenMinute=false;
                    for (Result row : scannerLastTenMinute) {
                        byte[] valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
                        if (valueBytes != null) {
                            isLastTenMinute = true;
                            //System.out.println("value:"+Bytes.toString(valueBytes));
                        }
                    }
                    
                    if(isLastTenMinute){
                    
                        ArrayList<HashMap<String,String>> minimumResultsList = new GenericCalculation().processScanedDataForGeneration("TotalProduction", table.getScanner(scan), startDateInSeconds, endDateInSeconds,"min");
                        //new InsertTenMinuteAggregate().insertTenMinuteAggregate(resultListMap);

                        ArrayList<HashMap<String,String>> maximumResultsList = new GenericCalculation().processScanedDataForGeneration("TotalProduction", table.getScanner(scanLastTenMinute), startTenMinuteInSeconds, endTenMinuteInSeconds,"max");
                        //System.out.println("");
                        for(HashMap<String,String> maxMap:maximumResultsList){
                            for(HashMap<String,String> minMap:minimumResultsList){
                                if(maxMap.get("assetId").equalsIgnoreCase(minMap.get("assetId"))){
                                    HashMap<String,String> resultMap=new HashMap<>();
                                    resultMap.put("assetId",maxMap.get("assetId"));
                                    resultMap.put("siteId",maxMap.get("siteId"));
                                    double generationValue=Double.parseDouble(maxMap.get("max"))-Double.parseDouble(minMap.get("min"));
                                    resultMap.put("generationValue",Double.toString(generationValue));
                                    SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
                                    resultMap.put("date",sdf.format(startDate));
                                    resultListMap.add(resultMap);
                                }
                            }
                        }

                        System.out.println(resultListMap);
                        
                        new InsertTenMinuteAggregate().insertDailyGenerationAggregate(resultListMap);
                        
                        return "success";
                    }
                    else{
                        isColumnExists=false;
                    }
                } 
                if(!isColumnExists){
                    SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
                    System.out.println("ActivePower");
                    resultListMap=new GenericCalculation().fetchTenMinuteGenerationAggregatedData(sdf.format(startDate), "ActivePower");
                    System.out.println(resultListMap);
                    return "success";
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
    /*
    Calculates the 10 minute aggreagation for WindSpeed KPI
    */

    public String windSpeedKPICalculation(String startDateInString, String endDateInString) {
        try {

            System.out.println("Start Date: " + startDateInString);
            long startDate = KPICalculation.convertDate(startDateInString);
            System.out.println("End Date: " + endDateInString);
            long endDate = KPICalculation.convertDate(endDateInString);

            ArrayList<HashMap<String, String>> resultListMap = new ArrayList<HashMap<String, String>>();

            String result = null;

            try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
//                String rowKey = Long.toString(Long.MAX_VALUE-endDate);
//                String stopValue=Long.toString(Long.MAX_VALUE-startDate);

                String rowKey = Long.toString(startDate);
                String stopValue = Long.toString(endDate);

                Scan scan = new Scan().withStartRow(Bytes.toBytes(rowKey), true).withStopRow(Bytes.toBytes(stopValue), true);

                //Scan scan = new Scan();
                Table table = connection.getTable(TableName.valueOf(tableId));

                resultListMap = new GenericCalculation().processScanedData("WindSpeed", table.getScanner(scan), startDate, endDate);

                /*for (HashMap<String, String> mapData : resultListMap) {
                    double avg = Double.parseDouble(mapData.get("averageValue")) / 6;
                    mapData.put("averageValue", Double.toString(avg));
                }*/

                new InsertTenMinuteAggregate().insertTenMinuteAggregate(resultListMap);

                return "success";

            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    /*
    Calculates the 10 minute aggreagation for Actual Production KPI
    */

    public String actualProductionKPICalculation(String startDateInString, String endDateInString) {
        try {

            System.out.println("Start Date: " + startDateInString);
            long startDate = KPICalculation.convertDate(startDateInString);
            System.out.println("End Date: " + endDateInString);
            long endDate = KPICalculation.convertDate(endDateInString);

            ArrayList<HashMap<String, String>> resultListMap = new ArrayList<>();

            
            try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
//                String rowKey = Long.toString(Long.MAX_VALUE-endDate);
//                String stopValue=Long.toString(Long.MAX_VALUE-startDate);

                String rowKey = Long.toString(startDate);
                String stopValue = Long.toString(endDate);

                Scan scan = new Scan().withStartRow(Bytes.toBytes(rowKey), true).withStopRow(Bytes.toBytes(stopValue), true);

                //Scan scan = new Scan();
                Table table = connection.getTable(TableName.valueOf(tableId));

                resultListMap = new GenericCalculation().processScanedData("pad_PowerAC", table.getScanner(scan), startDate, endDate);

                System.out.println("Re:" + resultListMap);
                
                for (HashMap<String, String> mapData : resultListMap) {
                        double avg = Double.parseDouble(mapData.get("averageValue")) / 6;
                        mapData.put("averageValue", Double.toString(avg));
                    }

                //new InsertTenMinuteAggregate().insertTenMinuteAggregate(resultListMap);
                
                System.out.println("Inserting Successfully");
                
                return "success";

            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    /*
    Calculates the 10 minute aggreagation for Potential production when available KPI
    */
    public String potentialProductionWhenAvailableKPICalculation(String startDateInString, String endDateInString) {
        try {

            System.out.println("Start Date: " + startDateInString);
            long startDate = KPICalculation.convertDate(startDateInString);
            System.out.println("End Date: " + endDateInString);
            long endDate = KPICalculation.convertDate(endDateInString);

            ArrayList<HashMap<String, String>> listOfPotentialProductionAggregates = new ArrayList<HashMap<String, String>>();
            ArrayList<HashMap<String, String>> listOfSOKAggregates = new ArrayList<HashMap<String, String>>();
            ArrayList<HashMap<String, String>> resultList = new ArrayList<HashMap<String, String>>();
            
            startDateInString=startDateInString.replace("T", " ");
            endDateInString=endDateInString.replace("T", " ");
            
            listOfPotentialProductionAggregates=new DBUtils().fetchTenMinuteAggregates(startDateInString, endDateInString, "PotentialProduction");
            listOfSOKAggregates=new DBUtils().fetchTenMinuteAggregates(startDateInString, endDateInString, "SOK");

            
            for(HashMap<String,String> prodMap:listOfPotentialProductionAggregates){
                for(HashMap<String,String> sokMap:listOfSOKAggregates){
                    if(sokMap.get("assetId").equalsIgnoreCase(prodMap.get("assetId"))){
                        double result=Double.parseDouble(prodMap.get("avgValue"))/(Double.parseDouble(sokMap.get("avgValue")) * 600);
                        HashMap<String,String> resultMap=new HashMap<>();
                        
                        resultMap.put("siteId",prodMap.get("siteId"));
                        resultMap.put("assetId",prodMap.get("assetId"));
                        resultMap.put("tagName",prodMap.get("tagName"));
                        resultMap.put("averageValue",Double.toString(result));
                        resultMap.put("startDateTime",Long.toString(startDate));
                        resultMap.put("endDateTime",Long.toString(endDate));
                        resultMap.put("max","0");
                        resultMap.put("min","0");
                        
                        resultList.add(resultMap);
                    }
                }
            }

            new InsertTenMinuteAggregate().insertTenMinuteAggregate(resultList);

            return "success";

            
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public String potentialProductionKPICalculation(String startDateInString, String endDateInString) {
        try {

            System.out.println("Start Date: " + startDateInString);
            long startDate = KPICalculation.convertDate(startDateInString);
            System.out.println("End Date: " + endDateInString);
            long endDate = KPICalculation.convertDate(endDateInString);

            ArrayList<HashMap<String, String>> resultListMap = new ArrayList<HashMap<String, String>>();
            
            ArrayList<HashMap<String, String>> finalresultListMap = new ArrayList<HashMap<String, String>>();
            
            String result = null;

            try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
//                String rowKey = Long.toString(Long.MAX_VALUE-endDate);
//                String stopValue=Long.toString(Long.MAX_VALUE-startDate);

                String rowKey = Long.toString(startDate);
                String stopValue = Long.toString(endDate);

                Scan scan = new Scan().withStartRow(Bytes.toBytes(rowKey), true).withStopRow(Bytes.toBytes(stopValue), true);

                //Scan scan = new Scan();
                Table table = connection.getTable(TableName.valueOf(tableId));

                resultListMap = new GenericCalculation().processScanedDataBasedOnSiteId("WindSpeed", table.getScanner(scan), startDate, endDate);

                //System.out.println("Re:" + resultListMap);

                finalresultListMap=new GenericCalculation().fetchActivePower(resultListMap);
                
                System.out.println(finalresultListMap);
                new InsertTenMinuteAggregate().insertTenMinuteAggregateForPotental(finalresultListMap);

                return "success";

            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    /*
    Calculates the sum of all values passed in list . This is an utility function.
    */
    public static double sum(List<Double> list) {
        double sum = 0;
        sum = list.stream().map((i) -> i).reduce(sum, Double::sum);
        return sum;
    }
    public String technicalAvailability(String startDateInString, String endDateInString) {
        try {
            
            startDateInString=startDateInString.replace("T", " ");
            endDateInString=endDateInString.replace("T", " ");
            
            ArrayList<HashMap<String,String>> resultMap=new ArrayList<>();
            resultMap=new DBUtils().fetchSumOfTenMinuteAggregatesSOK(startDateInString,endDateInString,"SOK");
            
            new InsertTenMinuteAggregate().insertDailyTechnicalAssetDetails(resultMap);
            
            return null;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }
    
    public String recoveryRateKPICalculation(String startDateInString, String endDateInString) {
        try {

            System.out.println("Start Date: " + startDateInString);
            long startDate = KPICalculation.convertDate(startDateInString);
            System.out.println("End Date: " + endDateInString);
            long endDate = KPICalculation.convertDate(endDateInString);

            ArrayList<HashMap<String, String>> resultListMap = new ArrayList<>();
            HashMap<String,String> assetSiteIdMap=new HashMap<>();
            HashMap<String, Integer> assetonesMap = new HashMap<>();
            HashMap<String, Integer> assettotalMap = new HashMap<>();

            try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
//                String rowKey = Long.toString(Long.MAX_VALUE-endDate);
//                String stopValue=Long.toString(Long.MAX_VALUE-startDate);

                String rowKey = Long.toString(startDate);
                String stopValue = Long.toString(endDate);

                Scan scan = new Scan().withStartRow(Bytes.toBytes(rowKey), true).withStopRow(Bytes.toBytes(stopValue), true);

                //Scan scan = new Scan();
                Table table = connection.getTable(TableName.valueOf(tableId));
                
                ResultScanner resultScanner= table.getScanner(scan);
                
                for (Result row : resultScanner) {
                    byte[] valueBytesOfActivePower = row.getValue(Bytes.toBytes("tag"), Bytes.toBytes("ActivePower"));
                    byte[] valueBytesOfWindSpeed = row.getValue(Bytes.toBytes("tag"), Bytes.toBytes("WindSpeed"));
                    
                    if(valueBytesOfActivePower != null && valueBytesOfWindSpeed != null 
                            && Bytes.toDouble(valueBytesOfActivePower)>0.0 && Bytes.toDouble(valueBytesOfWindSpeed)>0.0){
                        if(assetonesMap.get(Bytes.toString(row.getRow()).split("#")[2]) != null){
                            int count = assetonesMap.get(Bytes.toString(row.getRow()).split("#")[2]);
                            count++;
                            assetonesMap.put(Bytes.toString(row.getRow()).split("#")[2], count);
                            assetSiteIdMap.put(Bytes.toString(row.getRow()).split("#")[2],Bytes.toString(row.getRow()).split("#")[1]);
                        }
                        else{
                            assetonesMap.put(Bytes.toString(row.getRow()).split("#")[2], 1);
                            assetSiteIdMap.put(Bytes.toString(row.getRow()).split("#")[2],Bytes.toString(row.getRow()).split("#")[1]);
                        }
                    }
                    if(assettotalMap.get(Bytes.toString(row.getRow()).split("#")[2]) != null){
                        int count = assettotalMap.get(Bytes.toString(row.getRow()).split("#")[2]);
                        count++;
                        assettotalMap.put(Bytes.toString(row.getRow()).split("#")[2], count);
                    }
                    else{
                        assettotalMap.put(Bytes.toString(row.getRow()).split("#")[2], 1);
                    }
                }
                for(Map.Entry<String,Integer> entry : assettotalMap.entrySet()){
                    HashMap<String,String> resultMap=new HashMap<>();
                    
                    int ones = assetonesMap.get(entry.getKey());
                    int total = assettotalMap.get(entry.getKey());
                    double ans = ((double)(ones)/(double)(total))*100.0;
                    
                    resultMap.put("assetId",entry.getKey());
                    resultMap.put("siteId",assetSiteIdMap.get(entry.getKey()));
                    resultMap.put("aggValue",Double.toString(ans));
                    resultMap.put("tagName","RecoveryRate");
                    
                    //resultMap.put("tagCounts",Integer.toString(entry.getValue().size()));
                    resultMap.put("dateInString",Long.toString(startDate));
                    resultMap.put("max_value","0");
                    resultMap.put("min_value","0");
                    
                    resultListMap.add(resultMap);
                }
                
                System.out.println("Re:" + resultListMap);
                
                new InsertTenMinuteAggregate().insertDailyAggregate(resultListMap);
                
                System.out.println("Inserting Successfully");
                
                return "success";

            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    public String lossOfProductionKPI(String startDateInString, String endDateInString) {
        try {
            
            ArrayList<HashMap<String,String>> resultList=new ArrayList<>();
            startDateInString=startDateInString.replace("T", " ");
            endDateInString=endDateInString.replace("T", " ");
            
            ArrayList<HashMap<String,String>> potentialProductionList=new ArrayList<>();
            potentialProductionList=new DBUtils().fetchTenMinuteAggregates(startDateInString,endDateInString,"PotentialProduction");
            
            ArrayList<HashMap<String,String>> potentialProductionWhenAvailableList=new ArrayList<>();
            potentialProductionWhenAvailableList=new DBUtils().fetchTenMinuteAggregates(startDateInString,endDateInString,"PotentialProductionWhenAvailable");
            
            for(HashMap<String,String> potentialMap: potentialProductionList)
            {
                for(HashMap<String,String> potentialAvailableMap: potentialProductionWhenAvailableList){
                    if(potentialAvailableMap.get("assetId").equalsIgnoreCase(potentialMap.get("assetId"))){
                        if(potentialAvailableMap.get("startDate").equalsIgnoreCase(potentialMap.get("startDate")) &&
                                potentialAvailableMap.get("endDate").equalsIgnoreCase(potentialMap.get("endDate"))){
                            double productionLoss=Double.parseDouble(potentialMap.get("potentialMap")) -
                                Double.parseDouble(potentialAvailableMap.get("avgValue"));
                            HashMap<String,String> resultMap=new HashMap<String,String>();
                            resultMap.put("averageValue",Double.toString(productionLoss));
                            resultMap.put("assetId",potentialMap.get("assetId"));
                            resultMap.put("siteId",potentialMap.get("siteId"));
                            resultMap.put("startDateTime",potentialMap.get("startDate"));
                            resultMap.put("endDateTime",potentialMap.get("endDate"));
                            resultMap.put("tagName",potentialMap.get("tagName"));
                            resultMap.put("max","0");
                            resultMap.put("min","0");
                            
                            resultList.add(resultMap);
                        }
                    }
                }
            }
            new InsertTenMinuteAggregate().insertTenMinuteAggregate(resultList);

            return null;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }
    public String underOverPerformanceKPI(String startDateInString, String endDateInString) {
        try {
            
            ArrayList<HashMap<String,String>> resultList=new ArrayList<>();
            startDateInString=startDateInString.replace("T", " ");
            endDateInString=endDateInString.replace("T", " ");
            
            ArrayList<HashMap<String,String>> potentialProductionList=new ArrayList<>();
            potentialProductionList=new DBUtils().fetchTenMinuteAggregates(startDateInString,endDateInString,"PotentialProduction");
            
            ArrayList<HashMap<String,String>> actualProductionList=new ArrayList<>();
            actualProductionList=new DBUtils().fetchTenMinuteAggregates(startDateInString,endDateInString,"ActivePower");
            
            for(HashMap<String,String> potentialMap: potentialProductionList)
            {
                for(HashMap<String,String> actualProductionMap: actualProductionList){
                    if(actualProductionMap.get("assetId").equalsIgnoreCase(potentialMap.get("assetId"))){
                        if(actualProductionMap.get("startDate").equalsIgnoreCase(potentialMap.get("startDate")) &&
                                actualProductionMap.get("endDate").equalsIgnoreCase(potentialMap.get("endDate"))){
                            double productionLoss=Double.parseDouble(potentialMap.get("potentialMap")) -
                                Double.parseDouble(actualProductionMap.get("avgValue"));
                            HashMap<String,String> resultMap=new HashMap<>();
                            resultMap.put("averageValue",Double.toString(productionLoss));
                            resultMap.put("assetId",potentialMap.get("assetId"));
                            resultMap.put("siteId",potentialMap.get("siteId"));
                            resultMap.put("startDateTime",potentialMap.get("startDate"));
                            resultMap.put("endDateTime",potentialMap.get("endDate"));
                            resultMap.put("tagName",potentialMap.get("tagName"));
                            resultMap.put("max","0");
                            resultMap.put("min","0");
                            
                            resultList.add(resultMap);
                        }
                    }
                }
            }
            new InsertTenMinuteAggregate().insertTenMinuteAggregate(resultList);

            return null;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }
}
