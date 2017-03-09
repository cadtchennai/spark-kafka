package com.techmahindra.vehicletelemetry.utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DumpCarEventsData {

	public void dumpEventsData() {
		Properties props = new Properties();
		Connection connection = null;
		Statement stmt = null;
		Logger logger = Logger.getLogger("DumpCarEventsData");
		try {
			props.load(DumpCarEventsData.class.getClassLoader().getResourceAsStream("vt.properties"));
			connection = getDBConnection(props);
		        connection.setAutoCommit(false);
		        stmt = connection.createStatement();
		        stmt.execute("CREATE TABLE IF NOT EXISTS CAR(vin varchar(20),"+
								"model varchar(30),"+
								"timestamp timestamp,"+
								"outsideTemp real,"+
								"engineTemp real,"+
								"speed real,"+
								"fuel real,"+
								"engineOil real,"+
								"tirePressure real,"+
								"odometer bigint,"+
								"city varchar(20),"+
								"accPedalPos int,"+
								"parkBrakeStatus int,"+
								"headlampStatus int,"+
								"brakePedalStatus int,"+
								"transGearPosition varchar(10),"+
								"ignitionStatus int,"+
								"windshieldWiperStatus int,"+
								"abs int)");
		        
		        String csvPath = "D:\\Sample_Data\\careventsdata\\careventsdata\\rawcareventstream";
		        String[] csvFiles = new String[]{
		        		"2080383215_33731b26e90f4669b71cfa2df85d3012_1.csv",
		        		"2080383215_63a0aef2ca984bb18c6dab552eae6b50_1.csv",
		        		"2080383215_6a2ed936c5614837a35ce1f8df1551e1_1.csv",
		        		"2080383215_7443ee0a2c914257934d34312be5dd38_1.csv",
		        		"2080383215_84900e51f4c84ada9018431d78ce4875_1.csv",
		        		"2080383215_9c80515754db4bb085754c4beb593d4f_1.csv",
		        		"2080383215_a2a770c27da642ee84a6306ef146eaf6_1.csv",
		        		"2080383215_c9a9f0d821ce4ba49596b7a26a2e5c2f_1.csv",
		        		"2080383215_f04a00196fa440f1b78686c8a3c01027_1.csv",
		        		"2080383215_f2f05d60bc4a4683ba7e1712af6127cc_1.csv",
		        		"2080383215_f4a20b74e7af490fbf0fc51bca68c38b_1.csv",
		        		"2080383215_fad06a5bcb1b4fbebec0c17711dc0824_1.csv",
		        		"824507016_30dc7f8019f24402a2df1a49c21689bf_1.csv",
		        		"824507016_5244858dc7d84f0f9a47ffbf3692e7db_1.csv",
		        		"824507016_77fddce51ace464f8dcd963cad9c796b_1.csv",
		        		"824507016_84390024f6b242a987805492bc92e521_1.csv"
		        };
		        
		        for(String csvFile: csvFiles) {
		        	int count = stmt.executeUpdate("INSERT INTO CAR SELECT * FROM CSVREAD('" + csvPath + "\\" + csvFile + "', null, null)");
		        	logger.log(Level.INFO, "Inserted " + count + " records from " + csvFile);
		        }
	        
		} catch(IOException e) {
			logger.log(Level.SEVERE, "IOE", e);
		} catch (SQLException|ClassNotFoundException e) {
			logger.log(Level.SEVERE, "SQLE", e);
		} finally {
			if(stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					logger.log(Level.SEVERE, "SQLE", e);
				}
			if(connection != null)
				try {
					connection.close();
				} catch (SQLException e) {
					logger.log(Level.SEVERE, "SQLE", e);
				}
		}
	}
	
	private static Connection getDBConnection(Properties props) throws ClassNotFoundException, SQLException {
		Logger logger = Logger.getLogger("DumpCarEventsData");
        Connection dbConnection;
        try {
            Class.forName(props.getProperty("JDBC_DRIVER"));
        } catch (ClassNotFoundException e) {
        	logger.log(Level.SEVERE, "CNFE", e);
			throw e;
        }
        try {
            dbConnection = DriverManager.getConnection(props.getProperty("JDBC_CONN_STRING"), 
            											props.getProperty("JDBC_USER"), 
            											props.getProperty("JDBC_PASSWORD"));
        } catch (SQLException e) {
        	logger.log(Level.SEVERE, "SQLE", e);
			throw e;
        }
        return dbConnection;
    }
	
	public static void main(String[] args) throws Exception {
		(new DumpCarEventsData()).dumpEventsData();
	}
}
