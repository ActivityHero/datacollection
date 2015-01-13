package com.ah.scraper.export;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ah.scraper.common.Constants;
import com.ah.scraper.common.DBConnection;
import com.ah.scraper.common.ScraperUtil;
import com.ah.scraper.dao.ActivityDAO;
import com.ah.scraper.dao.ProviderDAO;
import com.ah.scraper.dto.ActivityDTO;
import com.ah.scraper.dto.ProviderDTO;
import com.mysql.jdbc.StringUtils;
import com.sun.corba.se.impl.orbutil.closure.Constant;

public class Upload {
	
	private static Logger LOG = LoggerFactory.getLogger(Upload.class);
	private static String site = "";
	private static boolean uploadCSV = false;
	private static boolean changeAgeToMonths = true;
	private static DBConnection dbCon = new DBConnection();
	public static void main(String[] args) {  
		if(uploadCSV){
			changeAgeToMonths = false;
			uploadCSV();
		}else{
			if(site.isEmpty()){
				ScraperUtil.log("Please specify which site to upload");
			}else{
				LOG.info("Started uploading records for site="+site);
				uploadFromDB();
				LOG.info("Finished uploading records for site="+site);
			}
		}
	}
	
	private static void uploadCSV(){
		String[] csvs = {};
		try{
			for (String csv : csvs) {
				ScraperUtil.log("Uploading csv file "+csv);
				ReadCSV readCsv = new ReadCSV(csv);
				ArrayList<ProviderDTO> providers = readCsv.parse();
				if(providers != null && providers.size() > 0){
					for (ProviderDTO providerDTO : providers) {
						if(providerDTO.getProvider_name() == null || providerDTO.getProvider_name().isEmpty()){
							continue;
						}
						uploadRecord(providerDTO, csv);
					}
				}
				LOG.info("Finished uploading csv file "+csv);
			}
			
		}catch(Exception e){
			LOG.error("err",e);
		}
	}
	private static void uploadFromDB(){
		ProviderDAO providerDao = new ProviderDAO(dbCon);
		int currentRecord = 0;
		int limit = 10;
		boolean hasMoreData = true;
		while(hasMoreData){
			hasMoreData = false;
			ArrayList<ProviderDTO> providers = providerDao.getProvidersToBeExported(site, currentRecord, limit);
			if(providers != null && providers.size() == limit){
				hasMoreData = true;
			}
			
			for (ProviderDTO providerDTO : providers) {
				uploadRecord(providerDTO, site);
				ScraperUtil.sleep();
			}
			
			currentRecord += limit;
		}
	}
	
	private static void uploadRecord(ProviderDTO pDto, String source){
		ProviderDAO providerDao = new ProviderDAO(dbCon);
		
		try {
			JSONObject data = getJsonData(pDto, source);
			ScraperUtil.log(data);
			LOG.info(data.toString());
			@SuppressWarnings("deprecation")
			DefaultHttpClient httpClient = new DefaultHttpClient();
			HttpPost postRequest = new HttpPost(Constants.UPLOAD_URL);
			
			StringEntity input = new StringEntity(data.toString(), Charset.forName("UTF-8"));
			input.setContentType("application/json; charset=utf-8");
			postRequest.setEntity(input);
 
			HttpResponse response = httpClient.execute(postRequest);
			int resStatus = response.getStatusLine().getStatusCode();
			ScraperUtil.log("status = "+resStatus);
			if (resStatus == 403) {
				LOG.equals("Permission error");
			}else{
				String newStatus = Constants.STATUS.EXPORT_ERROR.name();
				if(resStatus == 200)
					newStatus = Constants.STATUS.EXPORT_SUCCESS.name();
				HashMap<String, String> remarksMap = new HashMap<String, String>();
				remarksMap.put("responseStatus", Integer.toString(resStatus));
				remarksMap.put("uploadUrl", Constants.UPLOAD_URL);
				HttpEntity ent = response.getEntity();
				if(ent != null){
					String res = EntityUtils.toString(ent);
					ScraperUtil.log("Response from API - "+res);
					LOG.info("Response from API - "+res);
					try{
						remarksMap.put("response", StringUtils.escapeQuote(res, "'"));
					}catch(Exception e){
						LOG.error("err",e);
						remarksMap.put("response", res);
					}
					LOG.info("Response from API - {}",remarksMap.get("response"));
				}
				JSONObject remarksJson = new JSONObject(remarksMap);
				if(!uploadCSV)
					providerDao.markProviderAsExported(pDto.getSource_page_url(), newStatus, remarksJson.toString());
			}
		} catch (JSONException e) {
			LOG.error("err",e);
		} catch (IOException e) {
			LOG.error("err",e);
		}
	}
	
	private static JSONObject getJsonData(ProviderDTO pDto, String source) throws JSONException, UnsupportedEncodingException{
		HashMap<String, Object> resultMap = new HashMap<String, Object>();
		HashMap<String, Object> dataMap = new HashMap<String, Object>();
		ActivityDAO activityDao = new ActivityDAO(dbCon);
		resultMap.put("metadata", getMetaData(pDto, source));
		/**
		 * "data" => {
			"street_address" => "309 8th Ave",
			"city" => "San Mateo",
			"state" => "CA",
			"zip" => "94401-4260",
			"phone1" => "650-123-4567",
			"phone2" => "650-987-6543",
			"name" => "Imported Business",
			"description" => "Description goes here.",
			"email" => "imported@activityhero.com",
			"web_url" => "http://www.importedactivity.com",
			"fb_url" => "https://www.facebook.com/importedactivity",
			"online_reg" => Provider::PROVIDER_REG_LINK,
			"registration_link" => "http://www.importedactivity.com/register",
			# Add logo back in when mocked logo imports are available.
			# "logo_url" => "http://www.importedactivity.com/logo.png",
			"activities" => [{
			"activity_type" => "Class",
			"name" => "Class",
			"categories" => ["Dance", "Asia", "Unmapped"],
			"from_age" => "36",
			"to_age" => "48",
			"from_grade" => "1",
			"to_grade" => "2",
			"price" => "100",
			"alt_price" => "200",
			"price_type" => "per_week"
			}],
			"photos" => ["http://www.importedactivity.com/photo.png",
			"http://www.importedactivity.com/photo2.png"]
			},
			"metadata" => {
			"url" => "http://www.google.com/search?q=imported",
			"source" => "test"
			}
		 */
		if(pDto.getStreet() != null && !pDto.getStreet().isEmpty()){
			dataMap.put("street_address", pDto.getStreet().trim());
		}
		if(pDto.getCity() != null && !pDto.getCity().isEmpty()){
			dataMap.put("city", pDto.getCity().trim());
		}
		
		if(pDto.getState() != null && !pDto.getState().isEmpty()){
			dataMap.put("state", pDto.getState().trim());
		}
		
		if(pDto.getZip() != null && !pDto.getZip().isEmpty()){
			dataMap.put("zip", pDto.getZip().trim());
		}
		
		if(pDto.getPhone() != null && !pDto.getPhone().isEmpty()){
			String[] phones = pDto.getPhone().split("\t");
			//phone 1
			if(phones.length > 0){
				dataMap.put("phone1", phones[0].trim());
			}
			if(phones.length > 1){
				dataMap.put("phone2", phones[1].trim());
			}
		}
		
		if(pDto.getProvider_name() != null && !pDto.getProvider_name().isEmpty()){
			dataMap.put("name", pDto.getProvider_name().trim());
		}
		
		if(pDto.getDescription() != null && !pDto.getDescription().isEmpty()){
			dataMap.put("description", pDto.getDescription());
		}
		
		if(pDto.getEmail() != null && !pDto.getEmail().isEmpty()){
			dataMap.put("email", pDto.getEmail().trim());
		}
		
		if(pDto.getWebsite() != null && !pDto.getWebsite().isEmpty()){
			String website = pDto.getWebsite();
			if(website.contains("?ID=")){
				website = website.substring(0, website.indexOf("?"));
			}
			dataMap.put("web_url", website);
		}
		
		if(pDto.getLogo_url() != null && !pDto.getLogo_url().isEmpty()){
			dataMap.put("logo_url", pDto.getLogo_url().trim());
		}
		
		if(pDto.getFacebook_url() != null && !pDto.getFacebook_url().trim().isEmpty()){
			dataMap.put("fb_url", pDto.getFacebook_url());
		}
		
		if(pDto.getProgram_for() != null && !pDto.getProgram_for().isEmpty()){
			dataMap.put("program_for", pDto.getProgram_for().trim());
		}
		if(pDto.getProgram_type() != null && !pDto.getProgram_type().isEmpty()){
			dataMap.put("program_type", pDto.getProgram_type().trim());
		}
		
		if(pDto.getContact_person() != null && !pDto.getContact_person().isEmpty()){
			dataMap.put("contact_person", pDto.getContact_person().trim());
		}
		
		ArrayList<HashMap<String, Object>> activities = new ArrayList<HashMap<String, Object>>();;
		ArrayList<ActivityDTO> activityDtos = activityDao.getActivities(pDto.getProvider_id());
		if(activityDtos != null && activityDtos.size() > 0){
			for (ActivityDTO activityDTO : activityDtos) {
				activities.add(getActivityData(activityDTO));
			}
		}else{
			HashMap<String, Object> activityMap = getMockActivityData(pDto);
			activities.add(activityMap);
		}
		
		if(activities != null && activities.size() > 0){
			dataMap.put("activities", activities);
		}
		
		if(pDto.getPhoto_url() != null && !pDto.getPhoto_url().trim().isEmpty()){
			String[] photos = pDto.getPhoto_url().split("\t");
			ArrayList<String> photoUrls = new ArrayList<String>();
			for (String photo : photos) {
				photoUrls.add(photo.trim());
			}
			dataMap.put("photos", photoUrls);
		}
		if(pDto.getHas_camp() != null && !pDto.getHas_camp().isEmpty()){
			dataMap.put("has_camp", pDto.getSanitizedYesNo(pDto.getHas_camp()));
		}
		if(pDto.getHas_class() != null && !pDto.getHas_class().isEmpty()){
			dataMap.put("has_class", pDto.getSanitizedYesNo(pDto.getHas_class()));
		}
		
		if(pDto.getHas_birthday_party() != null && !pDto.getHas_birthday_party().isEmpty()){
			dataMap.put("has_birthday_party", pDto.getSanitizedYesNo(pDto.getHas_birthday_party()));
		}
		if(pDto.getHotLead() != null && !pDto.getHotLead().isEmpty()){
			dataMap.put("hot_lead", pDto.getHotLead());
		}
		if(pDto.getRegistration_type() != null && !pDto.getRegistration_type().trim().isEmpty()){
			dataMap.put("registration_type", pDto.getRegistration_type());
		}
		if(uploadCSV){
			if(pDto.getNotes() != null && !pDto.getNotes().trim().isEmpty()){
				dataMap.put("notes", pDto.getNotes());
			}
		}
		//set data
		resultMap.put("data", dataMap);
		resultMap.put("auth_email", Constants.AUTH_USER);
		resultMap.put("auth_password", Constants.AUTH_PASSWORD);
		JSONObject json = new JSONObject(resultMap);
		return json;
	}
	
	private static HashMap<String, String> getMetaData(ProviderDTO pDto, String source) throws JSONException{
		HashMap<String, String> meta = new HashMap<String, String>();
		meta.put("url", pDto.getSource_page_url());
		if(pDto.getIs_external_url().equals("Y") && pDto.getNotes() != null && !pDto.getNotes().trim().isEmpty()){
			JSONObject notesJson = new JSONObject(pDto.getNotes());
			ScraperUtil.log(notesJson.toString());
			if(notesJson.has("source_url") && !notesJson.isNull("source_url"))
				meta.put("url", notesJson.getString("source_url"));
		}
		meta.put("source", "satya-"+source);
		meta.put("scrapped_time", Long.toString(pDto.getCreated()));
		
		
		return meta;
	}
	
	private static HashMap<String, Object> getActivityData(ActivityDTO activityDto){
		HashMap<String, Object> activityMap = new HashMap<String, Object>();
		activityMap.put("name", activityDto.getName());
		activityMap.put("activity_type", activityDto.getActivityType());
		activityMap.put("price_tye", "per_session");
		ArrayList<String> categories = new ArrayList<String>();
		activityMap.put("categories", categories);
		activityMap.put("from_age", activityDto.getFrom_age());
		activityMap.put("to_age", activityDto.getTo_age());
		
		if(activityDto.getFrom_price() != -1.00){
			activityMap.put("price", activityDto.getFrom_price());
		}
		if(activityDto.getTo_price() != -1.00){
			activityMap.put("alt_price", activityDto.getTo_price());
		}
		
		return activityMap;
	}
	/**
	 * {
		"activity_type" => "Class",
		"name" => "Class",
		"categories" => ["Dance", "Asia", "Unmapped"],
		"from_age" => "36",
		"to_age" => "48",
		"from_grade" => "1",
		"to_grade" => "2",
		"price" => "100",
		"alt_price" => "200",
		"price_type" => "per_week"
		}
	 * @param pDto
	 * @return
	 * @throws JSONException 
	 */
	private static HashMap<String, Object> getMockActivityData(ProviderDTO pDto) throws JSONException{
		HashMap<String, Object> activityMap = new HashMap<String, Object>();
		activityMap.put("activity_type", "Camp");
		activityMap.put("name", "Camp");
		if(pDto.getHas_class().equals("Y")){
			activityMap.put("activity_type", "Class");
			activityMap.put("name", "Class");
		}else if(pDto.getHas_birthday_party().equals("Y")){
			activityMap.put("activity_type", "Party");
			activityMap.put("name", "Party");
		}
		ArrayList<String> categories = new ArrayList<String>();
		
		if(pDto.getActivities() != null && !pDto.getActivities().isEmpty()){
			String[] activities = pDto.getActivities().split("\t");
			for (String activity : activities) {
				categories.add(activity.trim());
			}
			if(categories.size() > 0){
				Set<String> uniqCats = new HashSet<String>(categories);
				activityMap.put("categories", uniqCats);
			}
		}else{
			activityMap.put("categories", categories);
		}
		
		if(pDto.getFrom_age() != -1){
			int age = pDto.getFrom_age();
			if(changeAgeToMonths)
				age = pDto.getFrom_age() * 12;
			activityMap.put("from_age", age);
		}
		
		if(pDto.getTo_age() != -1){
			int age = pDto.getTo_age();
			if(changeAgeToMonths)
				age = pDto.getTo_age() * 12;
			activityMap.put("to_age", age);
		}
		
		if(pDto.getFrom_grade() != null && !pDto.getFrom_grade().trim().isEmpty()){
			int gr = pDto.getFromGrade();
			if(gr != 100)
				activityMap.put("from_grade", gr);
		}
		
		if(pDto.getTo_grade() != null && !pDto.getTo_grade().trim().isEmpty()){
			int gr = pDto.getToGrade();
			if(gr != 100)
				activityMap.put("to_grade", gr);
		}
		
		if(pDto.getFrom_price() != -1.00){
			activityMap.put("price", pDto.getFrom_price());
		}
		if(pDto.getTo_price() != -1.00){
			activityMap.put("alt_price", pDto.getTo_price());
		}
		if(pDto.getPrice_type() != null && !pDto.getPrice_type().isEmpty()){
			activityMap.put("price_type", pDto.getPrice_type());
		}
		return activityMap;
	}
}
