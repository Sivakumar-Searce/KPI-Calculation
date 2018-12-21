/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.blp.kpicalculation;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import java.text.SimpleDateFormat;
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

/**
 *
 * @author Hemanth
 */
public class KPICalculation {
    
    public static String projectId = "platform-dev-blp";  // my-gcp-project-id
    public static String instanceId = "blp-dev-bt"; // my-bigtable-instance-id
    public static String tableId = "demo"; 
    
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
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss"); 
                
            Date date=formatter.parse(dateInString.replace("T" ," ").replace("Z",""));
            System.out.println(date.getTime());
           
            
            return date.getTime();
        }
        catch(Exception e){
            e.printStackTrace();
            long temp=-1;
            return temp;
        }
    }
    
    public static void main(String args[]){
        new KPICalculation().genericFunctionForKPI("2017-03-13T10:00:00Z", "2017-03-13T10:10:00Z", "-1", "ActivePower", "tag");
    }
}
