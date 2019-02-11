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
import javax.sql.DataSource;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;

/**
 *
 * @author Sivakumar
 */
public class DBConnection {
    
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    
    
    String instanceConnectionName = "platform-dev-blp:asia-south1:orion-dev-db";

                // TODO: fill this in
                // The database from which to list tables.
    String databaseName = "m2m_blp";
    String databaseName_midas = "midas_blp";
    String databaseName_pulsetest = "oriondb_pulse_test";

    // JDBC Database Credentials
    static final String JDBC_USER = "root";
    static final String JDBC_PASS = "ihatecloudsql";

    private static GenericObjectPool gPool = null;

    @SuppressWarnings("unused")
    public DataSource setUpPool() throws Exception {
            Class.forName(JDBC_DRIVER);
            
            String jdbcUrl = String.format(
	        "jdbc:mysql://google/%s?cloudSqlInstance=%s"
	            + "&socketFactory=com.google.cloud.sql.mysql.SocketFactory&useSSL=false",
	        databaseName,
	        instanceConnectionName);
            
            // Creates an Instance of GenericObjectPool That Holds Our Pool of Connections Object!
            gPool = new GenericObjectPool();
            gPool.setMaxActive(5);

            // Creates a ConnectionFactory Object Which Will Be Use by the Pool to Create the Connection Object!
            ConnectionFactory cf = new DriverManagerConnectionFactory(jdbcUrl, JDBC_USER, JDBC_PASS);

            // Creates a PoolableConnectionFactory That Will Wraps the Connection Object Created by the ConnectionFactory to Add Object Pooling Functionality!
            PoolableConnectionFactory pcf = new PoolableConnectionFactory(cf, gPool, null, null, false, true);
            return new PoolingDataSource(gPool);
    }
    
    @SuppressWarnings("unused")
    public DataSource setUpPoolMidas() throws Exception {
            Class.forName(JDBC_DRIVER);
            
            String jdbcUrl = String.format(
	        "jdbc:mysql://google/%s?cloudSqlInstance=%s"
	            + "&socketFactory=com.google.cloud.sql.mysql.SocketFactory&useSSL=false",
	        databaseName_midas,
	        instanceConnectionName);
            
            // Creates an Instance of GenericObjectPool That Holds Our Pool of Connections Object!
            gPool = new GenericObjectPool();
            gPool.setMaxActive(5);

            // Creates a ConnectionFactory Object Which Will Be Use by the Pool to Create the Connection Object!
            ConnectionFactory cf = new DriverManagerConnectionFactory(jdbcUrl, JDBC_USER, JDBC_PASS);

            // Creates a PoolableConnectionFactory That Will Wraps the Connection Object Created by the ConnectionFactory to Add Object Pooling Functionality!
            PoolableConnectionFactory pcf = new PoolableConnectionFactory(cf, gPool, null, null, false, true);
            return new PoolingDataSource(gPool);
    }
    @SuppressWarnings("unused")
    public DataSource setUpPoolPulseTest() throws Exception {
            Class.forName(JDBC_DRIVER);
            
            String jdbcUrl = String.format(
	        "jdbc:mysql://google/%s?cloudSqlInstance=%s"
	            + "&socketFactory=com.google.cloud.sql.mysql.SocketFactory&useSSL=false",
	        databaseName_pulsetest,
	        instanceConnectionName);
            
            // Creates an Instance of GenericObjectPool That Holds Our Pool of Connections Object!
            gPool = new GenericObjectPool();
            gPool.setMaxActive(5);

            // Creates a ConnectionFactory Object Which Will Be Use by the Pool to Create the Connection Object!
            ConnectionFactory cf = new DriverManagerConnectionFactory(jdbcUrl, JDBC_USER, JDBC_PASS);

            // Creates a PoolableConnectionFactory That Will Wraps the Connection Object Created by the ConnectionFactory to Add Object Pooling Functionality!
            PoolableConnectionFactory pcf = new PoolableConnectionFactory(cf, gPool, null, null, false, true);
            return new PoolingDataSource(gPool);
    }

    public GenericObjectPool getConnectionPool() {
            return gPool;
    }

    // This Method Is Used To Print The Connection Pool Status
    public void printDbStatus() {
            System.out.println("Max.: " + getConnectionPool().getMaxActive() + "; Active: " + getConnectionPool().getNumActive() + "; Idle: " + getConnectionPool().getNumIdle());
    }

    public static void main(String[] args) {
            /*ResultSet rsObj = null;
            Connection
            connObj = null;
            DBConnection jdbcObj = new DBConnection();
            
            ArrayList<HashMap<String,String>> mapList=new ArrayList<>();
            try{
                DataSource dataSource = jdbcObj.setUpPoolPulseTest();
                jdbcObj.printDbStatus();

                // Performing Database Operation!
                System.out.println("\n=====Making A New Connection Object For Db Transaction=====\n");
                connObj = dataSource.getConnection();
                jdbcObj.printDbStatus();
                
                Statement statement = connObj.createStatement();
                rsObj = statement.executeQuery("SELECT datafield6 FROM core_asset_details where asset_name='SACU_WT01'");
                
                
                while (rsObj.next()) {
                    //String assteName=(String)rsObj.getObject("asset_name");
                    String dateField=(String)rsObj.getObject("datafield6");
                    
                    //System.out.println("Datefiled:"+dateField);
                    
                    HashMap<String,String> mapData=new HashMap<String,String>();
                    
                    //mapData.put("assetName",assteName);
                    mapData.put("dateField",dateField.replace("-","_").replace(".","").toLowerCase());
                    
                    mapList.add(mapData);
                }
                
                
                System.out.println(mapList);
                
                new DBConnection().fetchalarmcode(mapList);
                
            }
            catch(Exception e){
                e.printStackTrace();
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
            jdbcObj.printDbStatus();*/
            new DBUtils().calculate24HoursWindSpeedAggData("","");
    }
    public String fetchalarmcode(ArrayList<HashMap<String,String>> mapList){
        ResultSet rsObj = null;
        Connection connObj = null;
        DBConnection jdbcObj = new DBConnection();
        try{
            DataSource dataSource = jdbcObj.setUpPoolMidas();
            jdbcObj.printDbStatus();

            // Performing Database Operation!
            System.out.println("\n=====Making A New Connection Object For Db Transaction=====\n");
            connObj = dataSource.getConnection();
            jdbcObj.printDbStatus();

            
            //System.out.println("Heree");
            for(HashMap<String,String> map: mapList)
            {
                //System.out.println("Heree1");
                Statement statement = connObj.createStatement();
                System.out.println("");
                rsObj = statement.executeQuery("SELECT fault_state,alarm_code,alarm_cat FROM "+map.get("dateField")+"");
                
                System.out.println("Query :"+ rsObj);
                
                while (rsObj.next()) {
                    String alarmCategory=(String)rsObj.getObject("alarm_cat");
                    String alarmDesc=(String)rsObj.getObject("alarm_desc");
                    System.out.println("Values :"+alarmCategory+" : "+alarmDesc);
                }
                
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
    /*public static void main(String[] args) {
            ResultSet rsObj = null;
            Connection
            connObj = null;
            PreparedStatement pstmtObj = null;
            DBConnection jdbcObj = new DBConnection();
            
            try{
                DataSource dataSource = jdbcObj.setUpPool();
                jdbcObj.printDbStatus();

                // Performing Database Operation!
                System.out.println("\n=====Making A New Connection Object For Db Transaction=====\n");
                connObj = dataSource.getConnection();
                jdbcObj.printDbStatus();
                
                Statement statement = connObj.createStatement();
                rsObj = statement.executeQuery("SELECT * FROM masterpowercurve where SITEID='AMB' ");
                System.out.println(rsObj);
                ResultSetMetaData rsmd = rsObj.getMetaData();
                int columnsNumber = rsmd.getColumnCount();
                while (rsObj.next()) {
                    for (int i = 1    ; i <= columnsNumber; i++) {
                        if (i > 1) System.out.print(",  ");
                        String columnValue = rsObj.getString(i);
                        System.out.print(columnValue + " " + rsmd.getColumnName(i));
                    }
                    System.out.println("");
                }
                while (rsObj.next()) {
                  System.out.println(rsObj.getString(1));
                }
            }
            catch(Exception e){
                e.printStackTrace();
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
            }
            jdbcObj.printDbStatus();
    }*/
}
