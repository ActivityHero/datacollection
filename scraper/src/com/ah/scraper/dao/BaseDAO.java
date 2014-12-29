package com.ah.scraper.dao;

import com.ah.scraper.common.DBConnection;

public class BaseDAO{ 
	 
	public DBConnection dbCon = null;
	protected BaseDAO(DBConnection dbCon){
		if(dbCon == null){
			dbCon = new DBConnection();
		}
		this.dbCon = dbCon;
	}
}
