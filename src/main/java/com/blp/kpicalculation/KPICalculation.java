/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.blp.kpicalculation;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
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
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author Hemanth
 */
public class KPICalculation {
    
    public static String projectId = "platform-dev-blp";  // my-gcp-project-id
    public static String instanceId = "blp-dev-bt"; // my-bigtable-instance-id
    public static String tableId = "validate"; 
    
    public static double sum(List<Double> list) {
        double sum = 0;
        sum = list.stream().map((i) -> i).reduce(sum, Double::sum);
        return sum;
    }
    
    
    public String genericFunctionForKPI(String startDateInString,String endDateInString,String assetId,String eventName,String familyGroup){
        try{
            long startDate= KPICalculation.convertDate(startDateInString);
            long endDate=KPICalculation.convertDate(endDateInString);

            byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(familyGroup);
            byte[] COLUMN_NAME = Bytes.toBytes(eventName);
            
            try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
            
                String rowKey = Long.toString(Long.MAX_VALUE-endDate);
                String stopValue=Long.toString(Long.MAX_VALUE-startDate);

                Scan scan = new Scan().withStartRow(Bytes.toBytes(rowKey),true).withStopRow(Bytes.toBytes(stopValue),true);
               
                Table table = connection.getTable(TableName.valueOf(tableId));

                ResultScanner scanner = table.getScanner(scan);
                
                Map<String, List<Double>> segList = new HashMap<String, List<Double>>();

                for (Result row : scanner) {
                    byte[] valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
                    if(valueBytes != null){
                        if(segList.get(Bytes.toString(row.getRow()).split("#")[1]) != null){
                            //System.out.println("Seg List Inner Loop:"+segList.get(Bytes.toString(row.getRow()).split("#")[1]));
                            List<Double> valueList= segList.get(Bytes.toString(row.getRow()).split("#")[1]);
                            valueList.add(Double.valueOf(Bytes.toString(valueBytes)));
                            segList.put(Bytes.toString(row.getRow()).split("#")[1],valueList);
                        }
                        else{
                            List<Double> valueList=new ArrayList<>();
                            valueList.add(Double.valueOf(Bytes.toString(valueBytes)));
                            segList.put(Bytes.toString(row.getRow()).split("#")[1],valueList);
                        }
                    }
                }

                //System.out.println(segList);

                ArrayList<HashMap<String,String>> listOfResults=new ArrayList<>();

                for(Map.Entry<String,List<Double>> entry : segList.entrySet()){
                    
                    HashMap<String,String> resultMap=new HashMap<>();
                    
                    double summationValue=KPICalculation.sum(entry.getValue());
                    double averageSum=summationValue/entry.getValue().size();

                    resultMap.put("assetId",entry.getKey());
                    resultMap.put("averageValue",Double.toString(averageSum));
                    resultMap.put("tagName",Bytes.toString(COLUMN_NAME));              
                    
                    listOfResults.add(resultMap);
                }

                System.out.println(listOfResults);
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
    
    public static void main(String args[]){
        
        DateTime startDate=new DateTime(2017,03,13,9,00,00);
        
        DateTime endDate=new DateTime(2017,03,13,9,30,00);
        
        DecimalFormat df = new DecimalFormat("00");
        int count=0;
        
        while(startDate.compareTo(endDate) != 0){
            DateTime tempDate=startDate;
            startDate=startDate.plusMinutes(10);
            
            String startYear=Integer.toString(tempDate.getYear());
            String startMonth=df.format(tempDate.getMonthOfYear());
            String startDay=df.format(tempDate.getDayOfMonth());
            String startHour=df.format(tempDate.getHourOfDay());
            String startMinute=df.format(tempDate.getMinuteOfHour());
            String startSeconds=df.format(tempDate.getSecondOfMinute());
            
            String endYear=Integer.toString(startDate.getYear());
            String endMonth=df.format(startDate.getMonthOfYear());
            String endDay=df.format(startDate.getDayOfMonth());
            String endHour=df.format(startDate.getHourOfDay());
            String endMinute=df.format(startDate.getMinuteOfHour());
            String endSeconds=df.format(startDate.getSecondOfMinute());
            
            String tempDateInString= startYear+"-"+startMonth+"-"+startDay+"T"+startHour+":"+startMinute+":"+startSeconds;
            String endDateInString= endYear+"-"+endMonth+"-"+endDay+"T"+endHour+":"+endMinute+":"+endSeconds;
            
    
            System.out.println("Generation KPI");
            new KPICalculation().generationKPI(tempDateInString, endDateInString);
            System.out.println("Actual Productioin KPI");
            new KPICalculation().actualProductionKPICalculation(tempDateInString, endDateInString);
            System.out.println("Potential Productioin KPI");
            new KPICalculation().potentialProductionKPICalculation(tempDateInString, endDateInString);
            System.out.println("Wind Speed KPI");
            new KPICalculation().windSpeedKPICalculation(tempDateInString, endDateInString);
           
            
            count++;
        }
        System.out.println("No. of Date range iterated:"+count);
    }
    
    public String generationKPI(String startDateInString,String endDateInString){
        try{
            byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("tag");
            byte[] COLUMN_NAME = Bytes.toBytes("TotalProduction_Raw");
            
            System.out.println("Data Proccesing Starts");
            
            System.out.println("Start Date: "+startDateInString);
            long startDate= KPICalculation.convertDate(startDateInString);
            System.out.println("End Date: "+endDateInString);
            long endDate=KPICalculation.convertDate(endDateInString);
//            long startDate=Long.parseLong("1489375800000");
//            long endDate=Long.parseLong("1489376400000");
            
            
            ArrayList<HashMap<String,String>> resultListMap=new ArrayList<HashMap<String,String>>();
            
            try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
//                String rowKey = Long.toString(Long.MAX_VALUE-endDate);
//                String stopValue=Long.toString(Long.MAX_VALUE-startDate);
                
                String rowKey = Long.toString(startDate);
                String stopValue=Long.toString(endDate);
                
                Scan scan = new Scan().withStartRow(Bytes.toBytes(rowKey),true).withStopRow(Bytes.toBytes(stopValue),true);
                
                //Scan scan = new Scan();
                
                Table table = connection.getTable(TableName.valueOf(tableId));
                
                ResultScanner scanner = table.getScanner(scan);
                
                boolean isColumnExists=false;
                
                for (Result row : scanner) {
                    byte[] valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
                    if(valueBytes != null){
                        isColumnExists=true;
                        //System.out.println("value:"+Bytes.toString(valueBytes));
                    }
                }
                if(isColumnExists){
                    resultListMap=new GenericCalculation().processScanedData("TotalProduction_Raw", table.getScanner(scan),startDate,endDate);
                    System.out.println("");
                    return "success";
                }
                else{
                    for (Result row : table.getScanner(scan)) {
                        byte[] valueBytes = row.getValue(COLUMN_FAMILY_NAME, Bytes.toBytes("TotalProduction"));
                        if(valueBytes != null){
                            isColumnExists=true;
                            //System.out.println("value:"+Bytes.toString(valueBytes));
                        }
                    }
                }
                
                if(isColumnExists){
                    resultListMap=new GenericCalculation().processScanedData("TotalProduction", table.getScanner(scan),startDate,endDate);
                    System.out.println("");
                    return "success";
                }
                else{
                    resultListMap=new GenericCalculation().processScanedData("ActivePower", table.getScanner(scan),startDate,endDate);
                    for(HashMap<String,String> mapData:resultListMap){
                        double avg=Double.parseDouble(mapData.get("averageValue"))/6;
                        mapData.put("averageValue",Double.toString(avg));
                    }
                    System.out.println("");
                    System.out.println("Result list after diveded by Six:"+resultListMap);
                    System.out.println("");
                    return "success";
                }
                
                
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            return null;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }
    
    public String windSpeedKPICalculation(String startDateInString,String endDateInString){
        try{
           
            System.out.println("Start Date: "+startDateInString);
            long startDate= KPICalculation.convertDate(startDateInString);
            System.out.println("End Date: "+endDateInString);
            long endDate=KPICalculation.convertDate(endDateInString);
            
            ArrayList<HashMap<String,String>> resultListMap=new ArrayList<HashMap<String,String>>();
            
            String result=null;
            
            try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
//                String rowKey = Long.toString(Long.MAX_VALUE-endDate);
//                String stopValue=Long.toString(Long.MAX_VALUE-startDate);
                
                String rowKey = Long.toString(startDate);
                String stopValue=Long.toString(endDate);

                Scan scan = new Scan().withStartRow(Bytes.toBytes(rowKey),true).withStopRow(Bytes.toBytes(stopValue),true);
                
                //Scan scan = new Scan();
                
                Table table = connection.getTable(TableName.valueOf(tableId));
                
                resultListMap=new GenericCalculation().processScanedData("WindSpeed", table.getScanner(scan),startDate,endDate);
                
                for(HashMap<String,String> mapData:resultListMap){
                        double avg=Double.parseDouble(mapData.get("averageValue"))/6;
                        mapData.put("averageValue",Double.toString(avg));
                    }
                
                System.out.println("");
                System.out.println("Result list after diveded by Six:"+resultListMap);
                System.out.println("");
               
                return "success";
                
            }
            catch(Exception e){
                e.printStackTrace();
                return null;
            }
        }
        catch(Exception e){
            e.printStackTrace();
            return null;    
        }
    }
    
    public String actualProductionKPICalculation(String startDateInString,String endDateInString){
        try{
           
            System.out.println("Start Date: "+startDateInString);
            long startDate= KPICalculation.convertDate(startDateInString);
            System.out.println("End Date: "+endDateInString);
            long endDate=KPICalculation.convertDate(endDateInString);
            
            ArrayList<HashMap<String,String>> resultListMap=new ArrayList<HashMap<String,String>>();
            
            String result=null;
            
            try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
//                String rowKey = Long.toString(Long.MAX_VALUE-endDate);
//                String stopValue=Long.toString(Long.MAX_VALUE-startDate);
                
                String rowKey = Long.toString(startDate);
                String stopValue=Long.toString(endDate);

                Scan scan = new Scan().withStartRow(Bytes.toBytes(rowKey),true).withStopRow(Bytes.toBytes(stopValue),true);
                
                //Scan scan = new Scan();
                
                Table table = connection.getTable(TableName.valueOf(tableId));
                
                resultListMap=new GenericCalculation().processScanedData("ActivePower", table.getScanner(scan),startDate,endDate);
                
                //System.out.println("Re:"+resultListMap);
                
                return "success";
                
            }
            catch(Exception e){
                e.printStackTrace();
                return null;
            }
        }
        catch(Exception e){
            e.printStackTrace();
            return null;    
        }
    }
    
    public String potentialProductionKPICalculation(String startDateInString,String endDateInString){
        try{
           
            System.out.println("Start Date: "+startDateInString);
            long startDate= KPICalculation.convertDate(startDateInString);
            System.out.println("End Date: "+endDateInString);
            long endDate=KPICalculation.convertDate(endDateInString);
            
            ArrayList<HashMap<String,String>> resultListMap=new ArrayList<HashMap<String,String>>();
            
            String result=null;
            
            try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
//                String rowKey = Long.toString(Long.MAX_VALUE-endDate);
//                String stopValue=Long.toString(Long.MAX_VALUE-startDate);
                
                String rowKey = Long.toString(startDate);
                String stopValue=Long.toString(endDate);

                Scan scan = new Scan().withStartRow(Bytes.toBytes(rowKey),true).withStopRow(Bytes.toBytes(stopValue),true);
                
                //Scan scan = new Scan();
                
                Table table = connection.getTable(TableName.valueOf(tableId));
                
                resultListMap=new GenericCalculation().processScanedDataBasedOnSiteId("WindSpeed", table.getScanner(scan),startDate,endDate);
                
                for(HashMap<String,String> mapData:resultListMap){
                    double avg=Double.parseDouble(mapData.get("averageValue"))/6;
                    mapData.put("averageValue",Double.toString(avg));
                }
                System.out.println("Re:"+resultListMap);
                
                new GenericCalculation().fetchActivePower(resultListMap);
                
                return "success";
                
            }
            catch(Exception e){
                e.printStackTrace();
                return null;
            }
        }
        catch(Exception e){
            e.printStackTrace();
            return null;    
        }
    }
 
}
