package com.ah.scraper.dao;

import java.sql.ResultSet;
import java.sql.SQLException; 
import java.util.ArrayList; 

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ah.scraper.common.DBConnection;

public class AddressDAO extends BaseDAO{
	private static Logger LOG = LoggerFactory.getLogger(AddressDAO.class);
 
	public AddressDAO(DBConnection dbCon) {
		super(dbCon); 
	}
	public ArrayList<String> getAllZips() {

		ArrayList<String> list = new ArrayList<String>();
		try { 
			String query = "select zip, longitude from address order by longitude";
			ResultSet rs = dbCon.query(query);

			while (rs.next()) {
				String zip = rs.getString("zip");

				if (!zip.matches("[a-zA-Z]")) {
					list.add(rs.getString("zip"));
				} 
			} 
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			LOG.error("err",e);
		}
		return list;
	}
}
