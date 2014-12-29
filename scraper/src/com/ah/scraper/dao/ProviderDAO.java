package com.ah.scraper.dao;
 
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ah.scraper.common.Constants;
import com.ah.scraper.common.DBConnection;
import com.ah.scraper.common.ScraperUtil;
import com.ah.scraper.dto.ProviderDTO;
import com.mysql.jdbc.Statement;

public class ProviderDAO extends BaseDAO{
	
	private static Logger LOG = LoggerFactory.getLogger(ProviderDAO.class);
	public ProviderDAO(DBConnection dbCon) {
		super(dbCon); 
	}
	
	public ArrayList<ProviderDTO> getProvidersToBeExported(String site, int start, int limit){
		String sql = "select * from provider where site='"+site+"' and status='"+Constants.STATUS.NEW+"' order by id asc limit "+limit;
		ArrayList<ProviderDTO> providers = new ArrayList<ProviderDTO>();
		ScraperUtil.log(sql);
		try { 
			ResultSet rs = dbCon.query(sql);
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		    
			while (rs.next()) {
				ProviderDTO dto = new ProviderDTO();
				dto.setProvider_id(rs.getLong("id"));
				dto.setProvider_name(rs.getString("provider_name"));
				dto.setSite(rs.getString("site"));
				dto.setWebsite(rs.getString("website"));
				dto.setEmail(rs.getString("email"));
				dto.setStreet(rs.getString("street"));
				dto.setCity(rs.getString("city"));
				dto.setZip(rs.getString("zip"));
				dto.setPhone(rs.getString("phone"));
				dto.setDescription(rs.getString("description"));
				dto.setState(rs.getString("state"));
				dto.setFrom_age(rs.getInt("from_age"));
				dto.setTo_age(rs.getInt("to_age"));
				dto.setContact_person(rs.getString("contact_person"));
				dto.setHas_class(rs.getString("has_class"));
				dto.setHas_camp(rs.getString("has_camp"));
				dto.setHas_birthday_party(rs.getString("has_birthday_party"));
				dto.setActivities(rs.getString("activities"));
				dto.setActivity_type(rs.getString("activity_type"));
				dto.setFrom_price(rs.getDouble("from_price"));
				dto.setTo_price(rs.getDouble("to_price"));
				dto.setRegistration_type(rs.getString("registration_type"));
				dto.setFacebook_url(rs.getString("facebook_url"));
				dto.setLogo_url(rs.getString("logo_url"));
				dto.setPhoto_url(rs.getString("photo_url"));
				dto.setLocation_name(rs.getString("location_name"));
				dto.setFrom_grade(rs.getString("from_grade"));
				dto.setTo_grade(rs.getString("to_grade"));
				dto.setProgram_type(rs.getString("program_type"));
				dto.setProgram_for(rs.getString("program_for"));
				dto.setNotes(rs.getString("notes"));
				dto.setSource_page_url(rs.getString("source_page_url"));
				dto.setIs_external_url(rs.getString("is_external_url"));
				dto.setStatus(rs.getString("status"));
				dto.setRemarks(rs.getString("remarks"));
				try {
					String created = rs.getString("created");
					String updated = rs.getString("updated");
					dto.setCreated(dateFormat.parse(created).getTime()/1000);
					dto.setUpdated(dateFormat.parse(updated).getTime()/1000);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					LOG.error("err",e);
				}
				providers.add(dto);
			} 
			rs.close();
		} catch (SQLException e) { 
			LOG.error("err",e);
		} 
		return providers;
	}
	public boolean markProviderAsExported(String pageUrl, String status, String remarks){
		if(pageUrl == null || pageUrl.isEmpty())
			return false;
		
		String sql = "update provider set status='"+status+"', remarks='"+remarks+"' where source_page_url_enc=md5(\""+pageUrl+"\")";
		boolean success = false;
		try{
			dbCon.update(sql);
			success = true;
		}catch(Exception e){
			LOG.error("err",e);
		}
		return success;
	}
	
	public boolean isRecordExist(String pageUrl) {
		String sql = "select source_page_url from provider where source_page_url_enc = md5(\""
				+ pageUrl + "\")";
		try { 
			ResultSet rs = dbCon.query(sql);
			while (rs.next()) {
				return true;
			} 
		} catch (SQLException e) { 
			LOG.error("err",e);
		} 
		return false;
	}
	
	public int addProvider(HashMap<String, Object> map) throws Exception {
		
		Object pageUrl = map.get(Constants.KEY_PAGE_URL);
		if (pageUrl != null) {
			String selectSQL = "select source_page_url_enc from provider where source_page_url_enc = md5(\""+ pageUrl + "\")";
			Connection connect  = dbCon.get();
			PreparedStatement preparedStatement = connect.prepareStatement(selectSQL);
			ResultSet rs = preparedStatement.executeQuery(selectSQL);
			while (rs.next()) {
				String deleteProviderSql = "DELETE from provider where source_page_url_enc=md5(?) and status='NEW'";
				PreparedStatement prest = connect.prepareStatement(deleteProviderSql);
				prest.setObject(1, pageUrl);
				prest.executeUpdate();
				break;
			}
		} else {
			throw new Exception("Page url must not be null");
		}
		Object site = map.get(Constants.KEY_SITE_NAME);
		Object name = map.get(Constants.KEY_PROVIDER_NAME);
		if (name == null)
			name = "";

		Object logo = map.get(Constants.KEY_LOGO_URL);
		if (logo == null)
			logo = "";

		Object photo = ScraperUtil.tabbedStrFromMap(map, Constants.KEY_PHOTO_URL);
		if (photo == null)
			photo = "";

		Object website = map.get(Constants.KEY_WEBSITE);
		if (website == null)
			website = "";

		Object description = map.get(Constants.KEY_DESCRIPTION);
		if (description == null)
			description = "";

		Object contactPerson = map.get(Constants.KEY_CONTACT_NAME);
		if (contactPerson == null)
			contactPerson = "";

		Object street = map.get(Constants.KEY_STREET_ADDRESS);
		if (street == null)
			street = "";

		Object city = map.get(Constants.KEY_CITY);
		if (city == null)
			city = "";

		Object state = map.get(Constants.KEY_STATE);
		if (state == null)
			state = "";

		Object zip = map.get(Constants.KEY_ZIP_CODE);
		if (zip == null)
			zip = "";

		Object phone = map.get(Constants.KEY_PHONE);
		if (phone == null)
			phone = "";

		Object email = map.get(Constants.KEY_EMAIL);
		if (email == null)
			email = "";

		Object hasClass = map.get(Constants.KEY_HAS_CLASS);
		if (hasClass == null)
			hasClass = "";

		Object hasBirthdayParty = map.get(Constants.KEY_HAS_BIRTHDAY_PARTY);
		if (hasBirthdayParty == null)
			hasBirthdayParty = "";

		Object activityType = map.get(Constants.KEY_ACTIVITY_TYPE);
		if (activityType == null)
			activityType = "";

		Object regType = map.get(Constants.KEY_REG_TYPE);
		if (regType == null)
			regType = "";

		Object fbUrl = map.get(Constants.KEY_FB_URL);
		if (fbUrl == null)
			fbUrl = "";

		Object notes = map.get(Constants.KEY_NOTES);
		if (notes == null)
			notes = "";

		Object location = ScraperUtil.locationFromMap(map);
		Object activities = ScraperUtil.tabbedStrFromMap(map, Constants.KEY_ACTIVITIES);

		Object hasCamp = map.get(Constants.KEY_HAS_CAMP);
		Object priceType = map.get(Constants.KEY_PRICE_TYPE);

		Object fromGrade = map.get(Constants.KEY_FROM_GRADE);
		if (fromGrade == null)
			fromGrade = "";

		Object toGrade = map.get(Constants.KEY_TO_GRADE);
		if (toGrade == null)
			toGrade = "";

		Object programType = map.get(Constants.KEY_PROGRAM_TYPE);
		if(programType == null)
			programType = "";
				
		Object programFor = map.get(Constants.KEY_PROGRAM_FOR);
		if(programFor == null)
			programFor = "";
		
		Object fp = map.get(Constants.KEY_FROM_PRICE);
		Float fromPrice = Float.valueOf(-1);
		;
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
		Object isExternalUrl = map.get(Constants.KEY_IS_EXTERNAL_URL);
		if(isExternalUrl == null)
			isExternalUrl = Constants.EXTERNAL_URL_NO; 
		
		Connection connect = dbCon.get();
		PreparedStatement preparedStatement = connect
				.prepareStatement("insert into  provider(provider_name, site, website, email, street, city, zip, phone, description, state, from_age, to_age, contact_person, "+
						"has_class, has_camp, has_birthday_party, activities, activity_type, from_price, to_price, price_type, registration_type, facebook_url, logo_url, photo_url, "+
						"location_name, from_grade, to_grade, program_type, program_for, notes, source_page_url, source_page_url_enc, is_external_url, status, remarks, created, updated) "+
						"values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, md5(?), ?, ?, ?, now(), now())", Statement.RETURN_GENERATED_KEYS);

		preparedStatement.setObject(1, name);
		preparedStatement.setObject(2, site);
		preparedStatement.setObject(3, website);
		preparedStatement.setObject(4, email);
		preparedStatement.setObject(5, street);
		preparedStatement.setObject(6, city);
		preparedStatement.setObject(7, zip);
		preparedStatement.setObject(8, phone);
		preparedStatement.setObject(9, description);
		preparedStatement.setObject(10, state);
		preparedStatement.setInt(11, fromAge);
		preparedStatement.setInt(12, toAge);
		preparedStatement.setObject(13, contactPerson);
		preparedStatement.setObject(14, hasClass);
		preparedStatement.setObject(15, hasCamp);
		preparedStatement.setObject(16, hasBirthdayParty);
		preparedStatement.setObject(17, activities);
		preparedStatement.setObject(18, activityType);
		preparedStatement.setFloat(19, fromPrice);
		preparedStatement.setFloat(20, toPrice);
		preparedStatement.setObject(21, priceType);
		preparedStatement.setObject(22, regType);
		preparedStatement.setObject(23, fbUrl);
		preparedStatement.setObject(24, logo);
		preparedStatement.setObject(25, photo);
		preparedStatement.setObject(26, location);
		preparedStatement.setObject(27, fromGrade);
		preparedStatement.setObject(28, toGrade);
		preparedStatement.setObject(29, programType);
		preparedStatement.setObject(30, programFor);
		preparedStatement.setObject(31, notes);
		preparedStatement.setObject(32, pageUrl);
		preparedStatement.setObject(33, pageUrl);
		preparedStatement.setObject(34, isExternalUrl);
		preparedStatement.setObject(35, Constants.STATUS.NEW.name());
		preparedStatement.setObject(36, "");
		preparedStatement.executeUpdate();
		ResultSet keys = preparedStatement.getGeneratedKeys();    
		keys.next();  
		int key = keys.getInt(1);
		dbCon.setHitCount(1);
		return key;
	}	
	
	/*public int getProviderID(Object providerName) throws Exception{
		String selectSQL = "select id from provider where provider_name = '"+ providerName +"'";
		Connection connect  = dbCon.get();
		PreparedStatement preparedStatement;
		preparedStatement = connect.prepareStatement(selectSQL);
	//	preparedStatement.setObject(1, providerName);
		ResultSet rs = preparedStatement.executeQuery(selectSQL);
		if(rs.next()){
			return rs.getInt(1);
		}else throw new Exception("Provider not exist.");
		
	}*/
}
