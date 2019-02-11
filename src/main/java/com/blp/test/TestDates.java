/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.blp.test;



import com.blp.kpicalculation.KPICalculation;
import java.util.ArrayList;
import java.util.HashMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author siva.kumar
 */
public class TestDates {
    public static void main(String args[]){
        /*String KPIname=args[0];
        String fromDate=args[1];
        String toDate=args[2];
        String siteId=args[3];
        
        System.out.println("KPI name:"+KPIname+" From Date: "+fromDate+" To date: "+toDate+" Site Id: "+siteId);
        
        KPICalculation.callKPIMethods(KPIname, fromDate, toDate, siteId);*/
        
        long start=1489536000000L;
        long end= 1489536012000L;
        
        long diff=end-start;
        
        diff=diff/1000;
        
        System.out.println("Diff : "+ diff);
    }
    public static void testDates(String[] args) {
        long timestamp=(long)1523578098;
        DateTime date=new DateTime(timestamp*1000);
        date=date.toDateTime(DateTimeZone.UTC);
        System.out.println("date  :"+date);
        
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        System.out.println("Formatted Date :"+formatter.print(date));
    }
    public static void test(String args[]){
        try{
        long startTimeStamp=(long)1493683200*1000;
        long endTimeStamp=(long)1493769600*1000;
        
        DateTime updateStartDate=new DateTime(startTimeStamp);
        DateTime updateEndDate=new DateTime(endTimeStamp);
        
        long startFaultTimeStamp=(long)1493721203*1000;
        long endFaultTimeStamp=(long)1493722150*1000;
        
        DateTime startFaultTime=new DateTime(startFaultTimeStamp);
        DateTime endFaultTime=new DateTime(endFaultTimeStamp);
        
     
        //System.out.println(updateStartDate);
        
        ArrayList<HashMap<String,String>> resultList=new ArrayList<>();
        
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
                    
                    resultList.add(dateMap);
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
                    
                    resultList.add(dateMap);
                    
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
                                
                                resultList.add(dateMapOne);
                            }
                            else{
                                DateTime tempStart=updateStartDate.plusMinutes(10);
                                DateTime tempEnd=tempStart.plusMinutes(10);
                                
                                HashMap<String,String> dateMapTwo=new HashMap<>();
                                
                                dateMapTwo.put("startDate",Long.toString(tempStart.getMillis()/1000));
                                dateMapTwo.put("endDate",Long.toString(tempEnd.getMillis()/1000));
                                dateMapTwo.put("uptime", Long.toString(600-diffInSeconds));
                                
                                resultList.add(dateMapTwo);
                                
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

                        resultList.add(dateMapTwo);

                        diffInSeconds=0;
                    }
                    break;
                }
                /*if(updateEndDate.compareTo(endFaultTime) > 0 || updateEndDate.compareTo(endFaultTime) == 0){
                    
                    DateTime tempStartTime=tempDate;
                    while(updateEndDate.compareTo(tempStartTime) != 0){
                    
                        DateTime tempEndTime=tempStartTime.plusMinutes(10);
                        
                        System.out.println("");
                        

                        if(tempStartTime.compareTo(endFaultTime) < 0 && tempEndTime.compareTo(endFaultTime) > 0){
                            long secondsDiffOfEndTimeToStartFault=(endFaultTime.getMillis()-tempStartTime.getMillis())/1000;
                            
                            dateMap.put("startDate",Long.toString(updateStartDate.getMillis()/1000));
                            dateMap.put("endDate",Long.toString(tempDate.getMillis()/1000));
                            dateMap.put("difference", Long.toString(secondsDiffOfEndTimeToStartFault));
                            
                            resultList.add(dateMap);
                        }
                        else{
                            dateMap.put("startDate",Long.toString(tempStartTime.getMillis()/1000));
                            dateMap.put("endDate",Long.toString(tempEndTime.getMillis()/1000));
                            dateMap.put("difference", "0");
                            
                            resultList.add(dateMap);
                        }
                        
                        tempStartTime=tempStartTime.plusMinutes(10);
                    }
                
                }*/
            }
            
            
            
            updateStartDate=updateStartDate.plusMinutes(10);
            
        }
        
        System.out.println(resultList);
        
        
                    
    }
        catch(Exception e){
            e.printStackTrace();
        }
    }
    
    
}
