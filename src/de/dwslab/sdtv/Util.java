package de.dwslab.sdtv;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Util {
	public static void removeTableIfExisting(String tableName) {
		Connection conn = ConnectionManager.getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			System.out.println("Error preparing table");
			e.printStackTrace();
		}
		try {
			String dropStatement = "DROP TABLE " + tableName;
			stmt.execute(dropStatement);
		} catch (SQLException e) {
			// this is not critical, it probably means that the table just did not exist
		}
	}
	
	public static void createIndex(String tableName, String columnName) {
		System.out.println("Creating index " + columnName + " on table " + tableName);
		Connection conn = ConnectionManager.getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			System.out.println("Error preparing index creation");
			e.printStackTrace();
		}
		
		String query = "CREATE INDEX idx_" + tableName + "_" + columnName + " ON " + tableName + "(" + columnName + ")";
		try {
			stmt.execute(query);
		} catch (SQLException e) {
			System.out.println("Error during index creation");
			e.printStackTrace();
		}
	}
	
	public static void checkTable(String tableName) {
		System.out.println("~~~ SAMPLE FROM " + tableName+ " ~~~");
		Connection conn = ConnectionManager.getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			System.out.println("Trying to sample table " + tableName);
			e.printStackTrace();
		}
		
		String query = "SELECT * FROM " + tableName + " LIMIT 10";
		try {
			ResultSet RS = stmt.executeQuery(query);
			while(RS.next()) {
				String line = "";
				for(int i=0;i<RS.getMetaData().getColumnCount();i++)
					line+=RS.getString(i+1) + "\t";
				System.out.println(line);
			}
		} catch (SQLException e) {
			System.out.println("Error sampling table " + tableName);
			e.printStackTrace();
		}
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		
	}
	
	public static void printTable(String tableName) {
		System.out.println("~~~ CONTENTS FROM " + tableName+ " ~~~");
		Connection conn = ConnectionManager.getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			System.out.println("Trying to sample table " + tableName);
			e.printStackTrace();
		}
		
		String query = "SELECT * FROM " + tableName;
		try {
			ResultSet RS = stmt.executeQuery(query);
			while(RS.next()) {
				String line = "";
				for(int i=0;i<RS.getMetaData().getColumnCount();i++)
					line+=RS.getString(i+1) + "\t";
				System.out.println(line);
			}
		} catch (SQLException e) {
			System.out.println("Error printing table " + tableName);
			e.printStackTrace();
		}
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		
	}
}
