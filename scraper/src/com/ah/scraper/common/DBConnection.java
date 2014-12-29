package com.ah.scraper.common;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.sql.Connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBConnection {
	private static Logger LOG = LoggerFactory.getLogger(DBConnection.class);
	private Connection connection = null;
	
	private int hitCount = 0;
	/**
	 * @return the hitCount
	 */
	public int getHitCount() {
		return hitCount;
	}

	/**
	 * @param hitCount the hitCount to set
	 */
	public void setHitCount(int hitCount) {
		this.hitCount += hitCount;
	}

	public Connection get(){
		try{  
			if(this.hitCount > 25){
				close();
			}
			if(connection == null ){  
				Class.forName("com.mysql.jdbc.Driver");
				String connStr = DBConfig.DB_HOST+DBConfig.DB_NAME+"?user="+DBConfig.DB_USER
								 + "&password="+DBConfig.DB_PASSWORD;	
				this.connection = DriverManager.getConnection(connStr);	
				return this.connection;
			}
		}
		catch(Exception e){
			LOG.error("err",e);
		} 
		return connection; 
	}
	
	public void close(){
		try{
			if(connection != null){ 
				connection.close();
			}
		}
		catch(Exception e){
			LOG.error("err",e);
		}
		connection = null;
	} 
	
	
	public ResultSet query(String query){
		Connection con = this.get();
		Statement stmt = null;;
		ResultSet rs = null;
		try {
			if(con == null){
				ScraperUtil.log("Connection is null"+query);
			}
			stmt = con.createStatement();
			 rs = stmt.executeQuery(query);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			LOG.error("err",e);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("err",e);
		}
		hitCount++;
		return rs;
	}
	
	public int update(String query){
		Connection con = this.get();
		Statement stmt = null; 
		int ret = 0;
		try {
			stmt = con.createStatement();
			ret = stmt.executeUpdate(query);
		} catch (SQLException e) { 
			LOG.error("err",e);
		} catch (Exception e) { 
			LOG.error("err",e);
		}
		hitCount++;
		return ret;
	}	 
	
	public void finalize(){
		
	}
}
