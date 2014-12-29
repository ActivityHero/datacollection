package com.ah.scraper.scrapers;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
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
public class MySummerCampScraper implements IScraper{
	private static Logger LOG = LoggerFactory.getLogger(MySummerCampScraper.class);
	public static int currentRecord = 1;
    public static String SITE_HOME_URL = "http://www.mysummercamps.com/";
    public static String SOURCE = "mysummercamps";
	private DBConnection dbCon;
	
	public MySummerCampScraper(){
		dbCon = new DBConnection(); 
	}
	
	public void run() {
		try { 
			LOG.info("MySummerCampScraper run called");
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
		Document page = Jsoup.connect("http://www.mysummercamps.com/camps/summer-camp-guide.html").timeout(Constants.REQUEST_TIMEOUT).get();
		
		Element categoryList = page.getElementsByClass("categoryList").first();
		if(categoryList != null){
			Element statesRow = categoryList.getElementsByClass("row").first();
			Elements states = statesRow.getElementsByTag("li");
			if(states != null){
				for (Element stateElem : states) {
					Element stateLink = stateElem.getElementsByTag("a").first();
					String statePageUrl = stateLink.absUrl("href");
					LOG.info("State Url - "+statePageUrl);
					parseState(statePageUrl);
					ScraperUtil.sleep();
				}
			}
		}
		LOG.info("MySummerCampScraper finished");
	}
	
	public void parseState(String url) throws Exception{
		boolean hasMoreData = true;
		int pageNum = 1;
		while (hasMoreData) {
			hasMoreData = false;
			String pageUrl = url;
			if(pageNum > 1)
				pageUrl = url.replace(".html", "-more"+pageNum+".html");
			LOG.info("Page url - "+pageUrl);
			Document page = Jsoup.connect(pageUrl).timeout(Constants.REQUEST_TIMEOUT).get();
			Element searchResults = page.getElementsByClass("searchList").first();
			if(searchResults != null){
				Elements articles = searchResults.getElementsByTag("article");
				for (Element article : articles) {
					Element textHolder = article.getElementsByClass("textHolder").first();
					Element photo = article.getElementsByClass("photo").first();
					if(textHolder == null || photo == null){
						LOG.info("Skipping article");
						continue;
					}
					try{
						Element articleLink = textHolder.getElementsByClass("heading").first().getElementsByTag("a").first();
						if(articleLink != null){
							String articleUrl = articleLink.absUrl("href");
							boolean isExternalUrl = false;
							if(!articleUrl.contains(SITE_HOME_URL) || articleUrl.contains("jump.cgi"))
								isExternalUrl = true;
							
							if(isExternalUrl){
								parseRowWithExtSource(article, articleUrl, pageUrl);
							}else{
								parsePage(articleUrl, articleUrl);
							}
						}
					}catch(Exception e){
						LOG.error("err",e);
					}
					hasMoreData = true;
				}
			}
			pageNum++;
		}
		
	}
	
	public void parseRowWithExtSource(Element article, String articleUrl, String pageUrl) throws Exception{
		LOG.info("Processing external url");
		ProviderDAO providerDAO  = new ProviderDAO(dbCon);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put(Constants.KEY_SITE_NAME, SOURCE);
		Element textHolder = article.getElementsByClass("textHolder").first();
		Element providerNameElem = textHolder.getElementsByTag("header").first().getElementsByTag("h3").first();
		Element providerNameLink = providerNameElem.getElementsByTag("a").first();
		String providerName = providerNameLink.text().trim();
		String providerUrl = providerNameLink.absUrl("href");
		
		map.put(Constants.KEY_PAGE_URL, getWebSiteUrl(providerUrl));
		map.put(Constants.KEY_PROVIDER_NAME, providerName);
		map.put(Constants.KEY_WEBSITE, getWebSiteUrl(providerUrl));
		map.put(Constants.KEY_IS_EXTERNAL_URL, Constants.EXTERNAL_URL_YES);
		
		HashMap<String, String> notesMap = new HashMap<String, String>();
		notesMap.put("source_url", pageUrl);
		notesMap.put("other_url", articleUrl);
		JSONObject notes = new JSONObject(notesMap);
		map.put(Constants.KEY_NOTES, notes.toString());
		
		Element logoElem = article.getElementsByClass("photo").first().getElementsByAttribute("data-src").first();
		map.put(Constants.KEY_LOGO_URL, logoElem.attr("data-src"));
		Element location = textHolder.getElementsByClass("location").first();
		String loc = location.text().trim();
		updateCityState(loc, map);
		
		//phone
		Elements phoneElems = textHolder.getElementsByAttributeValue("itemprop", "telephone");
		if(phoneElems != null){
			ArrayList<String> phoneNumbers = new ArrayList<String>();
			for (Element phoneElem : phoneElems) {
				String phone = phoneElem.text().trim();
				if(phone != null && !phone.isEmpty())
					phoneNumbers.add(phone);
			}
			if(phoneNumbers.size() > 0){
				map.put(Constants.KEY_PHONE, phoneNumbers);
				map.put(Constants.KEY_PHONE, ScraperUtil.tabbedStrFromMap(map, Constants.KEY_PHONE));
			}
		}

		Element descElem = textHolder.getElementsByTag("p").first();
		if(descElem != null){
			String desc = descElem.text().trim();
			map.put(Constants.KEY_DESCRIPTION, desc);
		}
		
		Element moreInfo = article.getElementsByClass("moreInfo").first();
		if(moreInfo != null){
			Element infoList = moreInfo.getElementsByClass("infoList").first();
			Elements liElems = infoList.getElementsByTag("li");
			updateCampInfo(liElems, map);
		}
		
		LOG.debug("result : {}",new JSONObject(map).toString());
		providerDAO.addProvider(map);
	}
	
	private void updateCityState(String loc, HashMap<String, Object> map){
		String[] locTokens = loc.split(",");
		String city = locTokens[0].trim();
		map.put(Constants.KEY_CITY, city);
		if(locTokens.length > 1){
			String state = locTokens[1].trim();
			map.put(Constants.KEY_STATE, state);
		}
	}
	private void updateCampInfo(Elements liElems, HashMap<String, Object> map){
		if(liElems != null){
			for (Element li : liElems) {
				String text = li.text().trim();
				if(text.contains("Camp")){
					text = text.replaceFirst("Camp", "").trim();
					map.put(Constants.KEY_PROGRAM_TYPE, text);
				}else if(text.startsWith("Type")){
					text = text.replaceFirst("Type:", "").trim();
					map.put(Constants.KEY_PROGRAM_TYPE, text);
				}else if(text.startsWith("Age")){
					text = text.replaceFirst("Age:", "").trim();
					String[] agesTokens = text.split("-");
					map.put(Constants.KEY_FROM_AGE, agesTokens[0].trim());
					if(agesTokens.length > 1){
						map.put(Constants.KEY_TO_AGE,agesTokens[1].trim());
					}
				}else if(text.startsWith("Gender")){
					text = text.replaceFirst("Gender:", "").trim();
					if(text.equals("Coed")){
						map.put(Constants.KEY_PROGRAM_FOR, Constants.PROGRAM_FOR_COED);
					}else if(text.equals("Boys")){
						map.put(Constants.KEY_PROGRAM_FOR, Constants.PROGRAM_FOR_BOYS);
					}else if(text.equals("Girls")){
						map.put(Constants.KEY_PROGRAM_FOR, Constants.PROGRAM_FOR_GIRLS);
					}
				}
			}
		}
	}
	
	private String getWebSiteUrl(String url){
		String website = url;
		if(url.contains("jump.cgi")){
			try {
				Document page = Jsoup.connect(url).timeout(Constants.REQUEST_TIMEOUT).get();
				LOG.info("Going to get website from url - "+url);
				website = page.baseUri();
				//all the urls are going to same site for same provider 
				//so append id
				URL u = new URL(url);
				HashMap<String, String> params = getQueryMap(u.getQuery());
				if(params.containsKey("ID")){
					if(website.contains("?")){
						website += "&ID="+params.get("ID");
					}else{
						website += "?ID="+params.get("ID");
					}
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				LOG.error("err",e);
			}
		}
		LOG.info("website - "+website);
		return website;
	}
	
	private HashMap<String, String> getQueryMap(String query){
		HashMap<String, String> map = new HashMap<String, String>();
		if(query != null && !query.isEmpty()){
			String[] params = query.split("&");
			for (String param : params) {
				String[] pTokens = param.split("=");
				if(pTokens.length == 2){
					map.put(pTokens[0], pTokens[1]);
				}else{
					map.put(pTokens[0], "");
				}
			}
		}
		
		return map;
	}
	public void parsePage(String pageUrl, String baseUri) throws Exception {
		LOG.info("Detail page url - "+pageUrl);
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
		
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put(Constants.KEY_SITE_NAME, SOURCE);
		
		Element content = campDetails.getElementById("content");
		Element heading = content.getElementsByClass("pageHeading").first();
		Element providerNameLink = heading.getElementsByAttributeValue("itemprop", "name").first().getElementsByTag("a").first();
		String providerName = providerNameLink.text().trim();
		String website = providerNameLink.absUrl("href").trim();
		
		map.put(Constants.KEY_PAGE_URL, pageUrl);
		map.put(Constants.KEY_PROVIDER_NAME, providerName);
		map.put(Constants.KEY_WEBSITE, getWebSiteUrl(website));
		
		Element location = heading.getElementsByClass("msc-detail-loc").first().getElementsByTag("li").first();
		String loc = location.text().trim();
		updateCityState(loc, map);
		
		Element photoGallery = content.getElementsByClass("photoGallery").first();
		Elements thumbnails = photoGallery.getElementsByClass("gall-thumb");
		if(thumbnails != null){
			ArrayList<String> photoUrls = new ArrayList<String>();
			for(Element img : thumbnails){
				String photoUrl = img.absUrl("src").trim();
				if(photoUrl != null)
					photoUrls.add(photoUrl);
			}
			if(photoUrls.size() > 0)
				map.put(Constants.KEY_PHOTO_URL, photoUrls);
		}
		
		Element textHolder = content.getElementsByClass("textHolder").first();
		if(textHolder != null){
			Element moreInfo = textHolder.getElementsByClass("moreInfo").first();
			if(moreInfo != null){
				Element infoList = moreInfo.getElementsByClass("infoListMobile").first();
				Elements liElems = infoList.getElementsByTag("li");
				updateCampInfo(liElems, map);
			}
		}
		
		Elements phoneElems = textHolder.getElementsByClass("phone-icon");
		if(phoneElems != null){
			ArrayList<String> phoneNumbers = new ArrayList<String>();
			for (Element phoneElem : phoneElems) {
				String phone = phoneElem.text().trim();
				if(phone != null && !phone.isEmpty())
					phoneNumbers.add(phone);
			}
			if(phoneNumbers.size() > 0){
				map.put(Constants.KEY_PHONE, phoneNumbers);
				map.put(Constants.KEY_PHONE, ScraperUtil.tabbedStrFromMap(map, Constants.KEY_PHONE));
			}
		}
		
		Element descElem = content.getElementById("about");
		String desc = descElem.html();
		if(!desc.isEmpty()){
			map.put(Constants.KEY_DESCRIPTION, desc.trim());
		}
		
		Element activitiesElem = content.getElementById("activities");
		Element activitiesTable = activitiesElem.getElementsByClass("activitiesTable").first();
		Elements liElems = activitiesTable.getElementsByTag("li");
		if(liElems != null){
			ArrayList<String> activities = new ArrayList<String>();
			for (Element li : liElems) {
				String text = li.text().trim();
				if(text.contains(",")){
					String[] tokens = text.split(",");
					for (String activity : tokens) {
						activities.add(activity.trim());
					}
				}else{
					activities.add(text);
				}
			}
			map.put(Constants.KEY_ACTIVITIES, activities);
		}
		
		LOG.info("{}",new JSONObject(map));
		providerDAO.addProvider(map);
	}
	
}
