package com.ah.scraper.dao;
 
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.ah.scraper.common.DBConnection;

public class MainPageDAO extends BaseDAO{
	
	public MainPageDAO(DBConnection dbCon) {
		super(dbCon); 
	}	
	public String getHtmlPage(String url, boolean isChild) throws SQLException{
			
		String sql = "select page from main_page where url_enc = md5(\""
				+ url+"\")";
		if(isChild){
			sql = "select page from child_page where url_enc = md5(\""
					+ url+"\")";
		} 
		ResultSet rs = dbCon.query(sql);
		while (rs.next()) {

			String page = (String) rs.getObject("page");
			
			if(page.length() > 10)
				return page;
			break;
		}
		return null;
	}
	
	public void addHtmlPage(String url, String pageHtml, String source) throws Exception {
		// preparedStatements can use variables and are more efficient
	
		if (url != null) {
			String sql = "select url from main_page where url_enc = md5(\""+ url+"\")";
			Connection connect = dbCon.get();
			PreparedStatement preparedStmt = connect
					.prepareStatement(sql);
			ResultSet rs = preparedStmt.executeQuery(sql);
			if(rs.next()){
				// update
				String query = "update main_page set page = ? where url_enc = md5(?)";
			    preparedStmt = connect.prepareStatement(query);
			    preparedStmt.setObject(1, pageHtml);
			    preparedStmt.setObject(2, url);
			      // execute the java preparedstatement
			    preparedStmt.executeUpdate();
			    
			}else{
				// add new
				String query = "insert into  main_page (url_enc, url, page, site) values (md5(?), ?, ?, ?)";
			    preparedStmt = connect.prepareStatement(query);
			    preparedStmt.setObject(1, url);
			    preparedStmt.setObject(2, url);
			    preparedStmt.setObject(3, pageHtml);
			    preparedStmt.setObject(4, source);
			    preparedStmt.executeUpdate();
			}
			dbCon.setHitCount(1);
		} else {
			throw new Exception("source_url must not be null");
		}
	}
	
	public void addChildHtmlPage(String url, String parent_url,String pageHtml, String source) throws Exception {
		// preparedStatements can use variables and are more efficient

		if (url != null) {
			
			if (parent_url != null) {
				String sql = "select url from child_page where url_enc = md5('"+ url+"')";
				Connection connect = dbCon.get();
				PreparedStatement preparedStatement = connect
						.prepareStatement(sql);
				ResultSet rs = preparedStatement.executeQuery(sql);
				if(rs.next()){
					// update
					String query = "update child_page set parent_url_enc = md5(?), page = ? where url_enc = md5(?)";
				    preparedStatement = connect.prepareStatement(query);
				    preparedStatement.setObject(1, parent_url);
				    preparedStatement.setObject(2, pageHtml);
					preparedStatement.setObject(3, url);
				      // execute the java preparedstatement
					preparedStatement.executeUpdate();
				}else{
					// add new
					String query = "insert into  child_page (url_enc, url, parent_url_enc, page, site) values (md5(?), ?, md5(?), ?, ?)";
				    preparedStatement = connect.prepareStatement(query);
				    preparedStatement.setObject(1, url);
				    preparedStatement.setObject(2, url);
				    preparedStatement.setObject(3, parent_url);
				    preparedStatement.setObject(4, pageHtml);
				    preparedStatement.setObject(5, source);
					preparedStatement.executeUpdate();
				}
				dbCon.setHitCount(1);
			}
			
		} else {
			throw new Exception("url must not be null");
		}

	}	
}
