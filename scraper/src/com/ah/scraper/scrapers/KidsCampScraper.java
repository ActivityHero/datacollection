package com.ah.scraper.scrapers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import com.ah.scraper.common.Constants;
import com.ah.scraper.common.DBConnection;
import com.ah.scraper.common.ScraperUtil;
import com.ah.scraper.dao.MainPageDAO;
import com.ah.scraper.dao.ProviderDAO;

public class KidsCampScraper  implements IScraper {
	private static Logger LOG = LoggerFactory.getLogger(KidsCampScraper.class);
	public static int currentRecord = 1;
    public static String SITE_HOME_URL = "http://www.kidscamps.com";
    public static String SOURCE = "kidscamps";
	private DBConnection dbCon;
	
	public KidsCampScraper(){
		dbCon = new DBConnection(); 
	}
	
	public void run(){
		try { 
			LOG.info("Scraper run called");
			this.startScraping();
		} catch (Exception e) { 
			LOG.error("err",e);
		}
		//close connections
		try{
			dbCon.close();
		}
		catch(Exception e){
			LOG.error("err",e);
		}
	}   
    public void startScraping() throws Exception {
		LOG.info("KidsCampScraper launched");
		//MainPageDAO htmlDAO = new MainPageDAO(dbCon);
		boolean hasMoreData = true;

		while (hasMoreData) {
			hasMoreData = false;
			int from_record = KidsCampScraper.currentRecord;
			KidsCampScraper.currentRecord += 25;
			
			String pageUrl =  SITE_HOME_URL + "/kc_dba/owa/kids1_newdesign?category_name=&distance2=10&country=U.S.A.&new_search=1&firstrow="+from_record+"&allofthem=1";
			LOG.info(SOURCE+": Getting data from = "+pageUrl);
			ScraperUtil.sleep();
			
			Document page = Jsoup.connect(pageUrl)
					.timeout(Constants.REQUEST_TIMEOUT).get();
			
			Element resultWrapper = page.getElementById("results-wrapper");
			if(resultWrapper == null){
				LOG.info(SOURCE+": results-wrapper not found");
				break;
			}
			Elements results = resultWrapper.getElementsByClass("result");
			if(results != null && results.size() > 0){
				boolean isExternalUrl = false;
				for (Element item : results) {
					isExternalUrl = false;
					Element nameElem = item.getElementsByAttributeValue("itemprop", "name").first();
					if(nameElem == null || nameElem.hasClass("luar")){
						continue;
					}
					hasMoreData = true;
					Element aElem = nameElem.getElementsByTag("a").first();
					String url = getCleanUrl(aElem.attr("href"));
					
					if(url.contains("linkto.cgi") || !url.contains(SITE_HOME_URL))
						isExternalUrl = true;
					
					if(isExternalUrl){
						parseRowWithExtSource(item, pageUrl, url);
					}else{
						parsePage(url, url);
					}
				}
				
			}else{
				LOG.info(SOURCE+": No results found");
			}
		}
		LOG.info("ACACampScraper finished");
	}
    
    public String getCleanUrl(String url){
		url = url.replaceAll(" ", "+").replaceAll("\r\n","").replaceAll("’", "");
		if(!url.startsWith("http"))
			url = SITE_HOME_URL + url;
		return url;
	}
    
	public void parsePage(String pageUrl, String baseUri) throws Exception {
		MainPageDAO htmlDAO = new MainPageDAO(dbCon);
		ProviderDAO providerDAO  = new ProviderDAO(dbCon);
		Document campDetails = null;
		
		String pageHtml = htmlDAO.getHtmlPage(pageUrl, false);
		if(pageHtml == null){
			LOG.info("GET Camp: " + pageUrl);
			ScraperUtil.sleep();
			campDetails = Jsoup.connect(pageUrl).timeout(Constants.REQUEST_TIMEOUT).get();
			htmlDAO.addHtmlPage(pageUrl, campDetails.toString(),SOURCE);
		}else{
			campDetails = Jsoup.parse(pageHtml,baseUri);
			
		}
		
		HashMap<String, String> ageGradeMap = new HashMap<String, String>();
		ageGradeMap.put("College Freshman", "17");
		ageGradeMap.put("2nd Grade", "7");
		ageGradeMap.put("2nd grade", "7");
		ageGradeMap.put("12th Grade", "17");
		ageGradeMap.put("9th grade", "14");
		ageGradeMap.put("Entering 3rd Grade", "8");
		ageGradeMap.put("All", null);
		ageGradeMap.put("All Ages", null);
		ageGradeMap.put("18+", "18");
		ageGradeMap.put("3.5", "4");
		ageGradeMap.put("14 Years", "14");
		ageGradeMap.put("21 Months", "2");
		
		
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put(Constants.KEY_SITE_NAME, SOURCE);
		map.put(Constants.KEY_IS_EXTERNAL_URL, Constants.EXTERNAL_URL_NO);
		map.put(Constants.KEY_PAGE_URL, pageUrl);
		
		Element providerNameElem = campDetails.getElementsByClass("camp_name_h1").first();
		if(providerNameElem != null)
			map.put(Constants.KEY_PROVIDER_NAME, providerNameElem.text().trim());
		//website and address
		Element contactUsElem = campDetails.getElementById("contact-us");
		if(contactUsElem != null){
			//website
			Element websiteWrapperElem = contactUsElem.getElementsByClass("linkweb").first();
			if(websiteWrapperElem != null){
				Element websiteLinkElem = websiteWrapperElem.getElementsByTag("a").first();
				if(websiteLinkElem != null){
					String website = getWebSiteUrl(websiteLinkElem.attr("href").trim());
					if(website != null)
						map.put(Constants.KEY_WEBSITE, website);
				}
			}
			
			//address
			Element addressWrapperElem = contactUsElem.getElementsByClass("contact").first();
			if(addressWrapperElem != null){
				Elements listElem = addressWrapperElem.getElementsByTag("li");
				if(listElem != null){
					updateAddress(listElem.first(), map);
					//phone
					updatePhoneNumber(listElem.first(), map);
					//logo
					if(listElem.size() == 3){
						updateLogoUrl(listElem.last(), map);
					}
				}
			}
			
			Element descElem = campDetails.getElementsByClass("desc").first();
			if(descElem != null){
				//all paragraphs
				String desc = "";
				Elements paragraphs = descElem.getElementsByTag("p");
				for (Element p : paragraphs) {
					if(!p.text().trim().equals("")){
						desc += p.text().trim()+"\n";
					}
				}
				//description
				map.put(Constants.KEY_DESCRIPTION, desc.trim());
				
				//gender age
				Elements liElems = descElem.getElementsByTag("li");
				ArrayList<Float> allPrices = new ArrayList<Float>();
				for (Element liElem : liElems) {
					Element dt = liElem.getElementsByTag("dt").first();
					Element dd = liElem.getElementsByTag("dd").first();
					if(dt != null && dd != null) {
						String label = dt.text().trim();
						String value = dd.text().trim();
						if(label.equals("Gender:")){
							if(value.equals("Co-Educational"))
								map.put(Constants.KEY_PROGRAM_FOR, Constants.PROGRAM_FOR_COED);
							else if(value.equals("Girls"))
								map.put(Constants.KEY_PROGRAM_FOR, Constants.PROGRAM_FOR_GIRLS);
							else if(value.equals("Boys"))
								map.put(Constants.KEY_PROGRAM_FOR, Constants.PROGRAM_FOR_BOYS);
						}else if(label.equals("Maximum age:")){
							if(ageGradeMap.containsKey(value))
								value = ageGradeMap.get(value);
							map.put(Constants.KEY_TO_AGE, value);
						}else if(label.equals("Minimum age:")){
							if(ageGradeMap.containsKey(value))
								value = ageGradeMap.get(value);
							map.put(Constants.KEY_FROM_AGE, value);
						} else if(label.equals("Directors:")){
							map.put(Constants.KEY_CONTACT_NAME, value);
						} else if(label.contains("Cost")){
							value = value.replaceAll("\\$", "").replaceAll(",", "").replaceAll(" and up","").replaceAll("Euro", "");
							if(value.contains("wk")){
								map.put(Constants.KEY_PRICE_TYPE, "per week");
								value = value.replaceAll("/wk", "");
							}
							String[] prices = value.split("-");
							for (String p : prices) {
								try{
									allPrices.add(Float.valueOf(p.trim()));
								}catch(Exception e){
									LOG.error("err",e);
								}
								
							}
						}
					}
				}
				
				if(allPrices.size() > 0){
					Collections.sort(allPrices);
					map.put(Constants.KEY_FROM_PRICE, allPrices.get(0).toString());
					if(allPrices.size() > 1){
						map.put(Constants.KEY_TO_PRICE, allPrices.get(allPrices.size()-1).toString());
					}
					
				}
				//photo url
				Element galleryElem = descElem.getElementsByClass("gallery_main").first();
				if(galleryElem != null){
					Elements photos = galleryElem.getElementsByClass("gall-thumb");
					if(photos != null){
						ArrayList<String> photoUrls = new ArrayList<String>();
						for (Element photo : photos) {
							String photoUrl = photo.absUrl("src").trim();
							if(photoUrl != null && !photoUrl.isEmpty())
								photoUrls.add(photoUrl);
						}
						if(photoUrls.size() > 0){
							map.put(Constants.KEY_PHOTO_URL, photoUrls);
						}
					}
					
				}
				//activities
				Element activitiesListElem = descElem.getElementsByClass("list").first();
				if(activitiesListElem != null && activitiesListElem.tagName().equals("ul")){
					ArrayList<String> activities = new ArrayList<String>();
					Elements activitiesLiElem = activitiesListElem.children();
					for (Element liElem : activitiesLiElem) {
						activities.add(liElem.text().trim());
					}
					map.put(Constants.KEY_ACTIVITIES, activities);
				}
			}
			LOG.info("result : "+new JSONObject(map).toString());
			providerDAO.addProvider(map);
		}
		

	}

	public void parseRowWithExtSource(Element item, String pageUrl, String url)  throws Exception{
		ProviderDAO providerDAO  = new ProviderDAO(dbCon);
		Element nameElem = item.getElementsByClass("name").first();
		Element aElem = nameElem.getElementsByTag("a").first();
		String providerUrl = aElem.attr("href");
		String providerName = aElem.text().trim();
		
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put(Constants.KEY_SITE_NAME, SOURCE);
		map.put(Constants.KEY_PAGE_URL, getWebSiteUrl(providerUrl));
		map.put(Constants.KEY_PROVIDER_NAME, providerName);
		map.put(Constants.KEY_WEBSITE, getWebSiteUrl(providerUrl));
		map.put(Constants.KEY_IS_EXTERNAL_URL, Constants.EXTERNAL_URL_YES);
		
		HashMap<String, String> notesMap = new HashMap<String, String>();
		notesMap.put("source_url", pageUrl);
		notesMap.put("other_url", url);
		JSONObject notes = new JSONObject(notesMap);
		map.put(Constants.KEY_NOTES, notes.toString());
		
		updateLogoUrl(item, map);
		//address
		Element resultInfoElem = item.getElementsByClass("resultinfo").first();
		if(resultInfoElem != null){
			updateAddress(resultInfoElem, map);
			
			//description
			Element descElem = item.getElementsByAttributeValue("itemprop", "description").first();
			if(descElem != null)
				map.put(Constants.KEY_DESCRIPTION, descElem.html().trim());
			
			//phone
			updatePhoneNumber(item, map);
		}
		LOG.info("result : "+new JSONObject(map).toString());
		providerDAO.addProvider(map);
	}
	
	static void updateLogoUrl(Element item, HashMap<String, Object> map){
		Element logoWrapper = item.getElementsByClass("logoimg").first();
		String logoUrl = null;
		if(logoWrapper != null){
			Element logoImg = logoWrapper.getElementsByTag("img").first();
			if(logoImg != null){
				logoUrl = logoImg.absUrl("src").trim();
			}
		}
		if(logoUrl != null){
			map.put(Constants.KEY_LOGO_URL, logoUrl);
		}
	}
	
	static String getWebSiteUrl(String url){
		String website = null;
		if(url.contains("linkto.cgi")){
			String[] tokens = url.split("\\?");
			if(tokens.length > 1){
				tokens = tokens[1].split("&");
				website = tokens[0].trim();
			}
		}else {
			website = url;
		}
		return website;
	}
	
	static void updatePhoneNumber(Element item, HashMap<String, Object> map){
		Elements phones = item.getElementsByAttributeValue("itemprop", "telephone");
		Element phoneElem = null;
		if(phones == null){
			
		}
		ArrayList<String> phoneNumbers = new ArrayList<String>();
		
		for (Element phone : phones) {
			phoneElem = phone.getElementsByClass("phone").first();
			if(phoneElem != null){
				String phoneJs = phoneElem.attr("href");
				if(phoneJs.contains("javascript:void(0)"))
					phoneJs = phoneElem.attr("onclick");
				if(phoneJs != null){
					String[] phoneTokens = phoneJs.split(",");
					if(phoneTokens.length > 1){
						String phoneNumber = phoneTokens[1].replaceAll("'", "").replaceAll("\\)", "").trim();
						if(phoneNumber != null && !phoneNumber.isEmpty())
							phoneNumbers.add(phoneNumber);
					}
				}
			}
		}
		if(phoneNumbers.size() > 0){
			map.put(Constants.KEY_PHONE, phoneNumbers);
			map.put(Constants.KEY_PHONE, ScraperUtil.tabbedStrFromMap(map, Constants.KEY_PHONE));
		}
	}
	
	static void updateAddress(Element item, HashMap<String, Object> map){
		Element addressElem = item.getElementsByAttributeValue("itemprop", "address").first();
		String addressInfo = addressElem.html();
		//remove <br><strong></strong>
		addressInfo = addressInfo.replaceAll("<br><strong></strong>", "");
		String[] addressTokens = addressInfo.split("<br>");
		if(addressTokens.length < 2){
			LOG.info("Addrees Format exception: " + addressElem.baseUri());
			return;
		}
		
		
		String[] tokens = addressTokens[addressTokens.length-1].split(",");
		addressTokens[addressTokens.length-1] = null;
		if (tokens.length > 1) {
			String city = tokens[0];
			map.put(Constants.KEY_CITY, city);
			String stateZip = tokens[1].trim();
			String[] arr = stateZip.split(" ");
			if (arr.length > 0) {
				String state = arr[0];
				map.put(Constants.KEY_STATE, state);
			} 
			if (arr.length > 1) {
				String zip = arr[1];
				map.put(Constants.KEY_ZIP_CODE, zip);
			}
		}
		
		//street address
		String streetAddress = "";
		for (String t : addressTokens) {
			if(t != null){
				streetAddress += t.trim()+"\n";
			}
		}
		if(streetAddress != null)
			map.put(Constants.KEY_STREET_ADDRESS, streetAddress.trim());
	}
}
