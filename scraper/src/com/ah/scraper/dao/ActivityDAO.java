package com.ah.scraper.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ah.scraper.common.Constants;
import com.ah.scraper.common.DBConnection;
import com.ah.scraper.common.ScraperUtil;
import com.ah.scraper.dto.ActivityDTO;


public class ActivityDAO extends BaseDAO {

	public ActivityDAO(DBConnection dbCon){
		super(dbCon);
	}
	
	private static Logger LOG = LoggerFactory.getLogger(ActivityDAO.class);
	public void addActivity(HashMap<String, Object> map) throws Exception{
		
		Object providerId = map.get(Constants.KEY_PROVIDER_ID);
		Object activityName = map.get(Constants.KEY_ACTIVITY_NAME);
		if (activityName == null)
			activityName = "";
		
		Object description = map.get(Constants.KEY_DESCRIPTION);
		if (description == null)
			description = "";
		
		Object girlsOnly = map.get(Constants.KEY_GIRLS_ONLY);
		if(girlsOnly == null)
			girlsOnly = 0;
		
		Object boysOnly = map.get(Constants.KEY_BOYS_ONLY);
		if(boysOnly == null)
			boysOnly = 0;
		
		Object fp = map.get(Constants.KEY_FROM_PRICE);
		Float fromPrice = Float.valueOf(-1);
		if (fp != null) {
			String str = (String) fp;
			if(str.lastIndexOf(".") == str.length()-1)
				str = str.substring(0, str.length()-2);
			fromPrice = Float.valueOf(str);
		}

		Object tp = map.get(Constants.KEY_TO_PRICE);
		Float toPrice = Float.valueOf(-1);
		if (tp != null) {
			String str = (String) tp;
			if(str.lastIndexOf(".") == str.length()-1)
				str = str.substring(0, str.length()-2);
			toPrice = Float.valueOf(str);
		}

		Object ta = map.get(Constants.KEY_TO_AGE);
		Integer toAge = -1;
		if (ta != null && !ta.equals("null")) {
			String str = (String) ta;
			toAge = Integer.valueOf(str);
		}

		Object fa = map.get(Constants.KEY_FROM_AGE);
		Integer fromAge = -1;
		if (fa != null && !fa.equals("null")) {
			String from_age_str = (String) fa;
			fromAge = Integer.valueOf(from_age_str);
		}
		
		Connection connect = dbCon.get();
		PreparedStatement preparedStatement = connect.prepareStatement("insert into activities" +
				"(provider_id, name, description, from_age, to_age, from_price, to_price, boys_only, girls_only, created_at, updated_at)" +
				" values(?,?,?,?,?,?,?,?,?,now(), now())");
		preparedStatement.setObject(1, providerId);
		preparedStatement.setObject(2, activityName);
		preparedStatement.setObject(3, description);
		preparedStatement.setObject(4, fromAge);
		preparedStatement.setObject(5, toAge);
		preparedStatement.setObject(6, fromPrice);
		preparedStatement.setObject(7, toPrice);
		preparedStatement.setObject(8, boysOnly);
		preparedStatement.setObject(9, girlsOnly);
		
		preparedStatement.executeUpdate();
		dbCon.setHitCount(1);
		
	}
	
	public ArrayList<ActivityDTO> getActivities(long provider_id){
		ArrayList<ActivityDTO> activities = new ArrayList<ActivityDTO>();
		try{
			String selectSQL = "select * from activities where provider_id = "+ provider_id;
			ScraperUtil.log(selectSQL);
			Connection connect  = dbCon.get();
			PreparedStatement preparedStatement;
			preparedStatement = connect.prepareStatement(selectSQL);
			ResultSet rs = preparedStatement.executeQuery(selectSQL);
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			while(rs.next()){
				ActivityDTO dto = new ActivityDTO();
				dto.setprovider_id(rs.getInt("provider_id"));
				dto.setName(rs.getString("name"));
				dto.setDescription(rs.getString("description"));
				dto.setFrom_age(rs.getInt("from_age"));
				dto.setTo_age(rs.getInt("to_age"));
				dto.setFrom_price(rs.getFloat("from_price"));
				dto.setTo_price(rs.getFloat("to_price"));
				dto.setActivityType(rs.getString("activity_type"));
				dto.setProgram_for(rs.getString("program_for"));
				try {
					String created = rs.getString("created_at");
					String updated = rs.getString("updated_at");
					dto.setCreated_at(dateFormat.parse(created).getTime()/1000);
					dto.setUpdated_at(dateFormat.parse(updated).getTime()/1000);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					LOG.error("err",e);
				}
				activities.add(dto);
			}
		}catch (Exception e) {
			LOG.error("err",e);
		}
		return activities;
	}
}
