package com.ah.scraper.dao;
 
import java.sql.ResultSet;
import java.sql.SQLException; 
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ah.scraper.common.DBConnection;
 
public class ZipProcessedDAO extends BaseDAO{
 
	private Logger LOG = LoggerFactory.getLogger(ZipProcessedDAO.class);
	public ZipProcessedDAO(DBConnection dbCon) {
		super(dbCon); 
	}
	public void markProcessed(String zip, String site) throws SQLException { 
		String query = "select zip from zip_processed where site = '"+ site+"' and zip='"+zip+"'"; 
		ResultSet rs = dbCon.query(query);
		if(!rs.next()){ 
			query = "insert into  zip_processed (zip,site) values ('"+zip+"','"+site+"')";
			dbCon.update(query);
		}
		rs.close();
	}	
	public ArrayList<String> getUnprocessedZip(String site, int threadCnt) { 
		ArrayList<String> lst = new ArrayList<String>() ;
		try {
 			String query = "select zip from address where zip not in (select zip from zip_processed where site='"+site+"') order by rand() limit  "+threadCnt;
			ResultSet rs = dbCon.query(query); 
			while (rs.next()) { 
				lst.add(rs.getString("zip"));
			} 
			rs.close();
		} catch (SQLException e) { 
			LOG.error("err",e);
		}
		return lst;
	}
	
	public String getUnprocessedZip(String site) { 
		String zip = null;
		try {
 			String query = "select zip from address where zip not in (select zip from zip_processed where site='"+site+"') order by rand() limit 1";
			ResultSet rs = dbCon.query(query); 
			while (rs.next()) {
				zip = rs.getString("zip");  
			} 
			rs.close();
		} catch (SQLException e) { 
			LOG.error("err",e);
		}
		return zip;
	}
}
