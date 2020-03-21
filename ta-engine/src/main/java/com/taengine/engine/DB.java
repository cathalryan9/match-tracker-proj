package com.taengine.engine;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Scanner;

import scala.Tuple2;

public class DB {

	    public static void createNewDatabase(String fileName) {
	 
	        String url = fileName;
	 
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
	    	String url = fileName;
	   	 
	        try (Connection conn = DriverManager.getConnection(url)) {
	            if (conn != null) {
	            	Statement stmt = conn.createStatement();
	            	String sql = "DELETE FROM words";
	            	stmt.execute(sql);
	            }
	 
	        } catch (SQLException e) {
	            System.out.println(e.getMessage());
	        }
	    }
	    
	    public static String getDatabase(String fileName) {
	    	return "jdbc:sqlite:" + fileName;
	    }
	 
	    public static void write(Tuple2<String, Integer> jds, String dbName, String table) {

			String url = DB.getDatabase(dbName);
			Connection conn = null;

			try {
				conn = DriverManager.getConnection(url);
				// Does the word have to be updated or inserted
				String sqlSelectStr = "SELECT * FROM " + table + " WHERE word=" + "'" + jds._1 + "'";
				try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sqlSelectStr)) {
					if (rs.next()) {
						stmt.close();
						// Do an update
						String sqlUpdateStr = "UPDATE " + table + " SET count = count + ?, datetime = ? WHERE word = ?";
						PreparedStatement pstmt = conn.prepareStatement(sqlUpdateStr);
						pstmt.setInt(1, jds._2);
						pstmt.setString(2, LocalDateTime.now().toString());
						pstmt.setString(3, jds._1());
						pstmt.execute();
						pstmt.close();
					} else {
						stmt.close();
						String sql2 = "INSERT INTO " + table + "(word,count,datetime) VALUES(?,?,?) ";
						PreparedStatement pstmt = conn.prepareStatement(sql2);
						pstmt.setString(1, jds._1);
						pstmt.setInt(2, jds._2());
						pstmt.setString(3, LocalDateTime.now().toString());
						pstmt.execute();
						pstmt.close();
					}
				} catch (SQLException e) {
					System.out.println(e.getMessage());
				}
				conn.close();
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
	            createNewDatabase(getDatabase("test.db"));
	            
	            if (delete == true) {
	            	deleteDatabaseData("test.db");
	            }
	    	
	        
	    }
	

}
