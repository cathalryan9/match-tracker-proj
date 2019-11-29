package com.taengine.engine;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class DB {


	 
	/**
	 *
	 * @author sqlitetutorial.net
	 */	 
	    /**
	     * Connect to a sample database
	     *
	     * @param fileName the database file name
	     */
	    public static void createNewDatabase(String fileName) {
	 
	        String url = "jdbc:sqlite:../" + fileName;
	 
	        try (Connection conn = DriverManager.getConnection(url)) {
	            if (conn != null) {
	                DatabaseMetaData meta = conn.getMetaData();
	                System.out.println("The driver name is " + meta.getDriverName());
	                System.out.println("A new database has been created.");
	                String sql = "CREATE TABLE IF NOT EXISTS words (\n"
	                        + "    id integer PRIMARY KEY AUTOINCREMENT,\n"
	                        + "    word text NOT NULL UNIQUE,\n"
	                        + "	   count integer NOT NULL,\n"	
	                        + "    datetime text NOT NULL\n"
	                        + ");";
	                Statement stmt = conn.createStatement();
	                stmt.execute(sql);
	                sql = "CREATE TABLE IF NOT EXISTS words_by_interval (\n"
	                        + "    id integer PRIMARY KEY AUTOINCREMENT,\n"
	                        + "    word text NOT NULL,\n"
	                        + "	   count integer NOT NULL,\n"	
	                        + "    datetime text NOT NULL,s\n"
	                        + "    interval_in_minutes integer NOT NULL\n"
	                        + ");";
	                stmt = conn.createStatement();
		            stmt.execute(sql);
	            }
	            
	 
	        } catch (SQLException e) {
	            System.out.println(e.getMessage());
	        }
	    }
	    
	    public static void deleteDatabaseData(String fileName) {
	    	String url = "jdbc:sqlite:../" + fileName;
	   	 
	        try (Connection conn = DriverManager.getConnection(url)) {
	            if (conn != null) {
	            	String sql = "DELETE * FROM words";
	            }
	 
	        } catch (SQLException e) {
	            System.out.println(e.getMessage());
	        }
	    }
	 
	    /**
	     * @param args the command line arguments
	     */
	    public static void main(String[] args) {
	    	
	    	boolean delete = false;
	    	while(true) {
	    		Scanner scanner = new Scanner(System.in);
		    	System.out.println("Delete previous data? Y/N");
	            String line = scanner.nextLine();
	            
	            
	            if(line.equals("Y") | line.equals("y")) {
	            	delete = true;
	            
	            }
	            else if (!line.equals("N") && !line.equals("n")) {
	            	System.out.println("Input not recognized. ");
	            	continue;
	            }
	            scanner.close();
	            break;
	            }
	    	
	            // Will do nothing if db already exists
	            createNewDatabase("test.db");
	            
	            if (delete == true) {
	            	deleteDatabaseData("test.db");
	            }
	    	
	        
	    }
	

}
