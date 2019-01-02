package com.infy.gcp.DataExtractionBatch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class ConnectionUtil {
	
	static Connection connection = null;
	 public static Connection connectToOracle(String ipPortDb,String user,String password) throws SQLException {

		 
			
			
		 try {
				
				if (connection == null || connection.isClosed()) {
					Class.forName(MetadataDbConstants.ORACLE_DRIVER);
					
					String jdbc = "jdbc:oracle:thin:@"+ipPortDb;
					System.out.println(jdbc);
					connection = DriverManager.getConnection(jdbc, user, password);
				}
		 } catch (ClassNotFoundException | SQLException e) {
			   e.printStackTrace();
				throw new SQLException("Exception occured while connecting to mysql");
			}
	 
	
	 	System.out.println("connection succeeded");
		return connection;
	 
 }	 

}
