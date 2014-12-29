package com.ah.scraper.scrapers;

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

public class CampPageScraper  implements IScraper {
	private static Logger LOG = LoggerFactory.getLogger(CampPageScraper.class);
	public static String SITE_HOME_URL = "http://www.camppage.com";
	public static String SOURCE = "camppage";
	

	private DBConnection dbCon;
	
	public CampPageScraper(){
		dbCon = new DBConnection(); 
	}	
	
	public void run(){
		try { 
			LOG.info("CampPageScraper run called");
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
	public void startScraping() throws Exception{
		MainPageDAO htmlDAO = new MainPageDAO(dbCon);
		Document page = Jsoup.connect(SITE_HOME_URL)
				.timeout(Constants.REQUEST_TIMEOUT).get();

		Element stateSelector = page.getElementById("stateprovince");
		Elements states = stateSelector.getElementsByTag("option");
		for(Element stateOption : states){
			String statePageUrl = stateOption.absUrl("value");
			if(statePageUrl.contains("index.php")){
				continue;
			}
			
			String pageHtml = htmlDAO.getHtmlPage(statePageUrl, false);
			if(pageHtml == null){
				ScraperUtil.sleep();
				page = Jsoup.connect(statePageUrl).timeout(Constants.REQUEST_TIMEOUT).get();
				pageHtml = page.toString();
				htmlDAO.addHtmlPage(statePageUrl, pageHtml, SOURCE);
			}
			
			parseHtml(statePageUrl, pageHtml);
		}	
		LOG.info("CampPageScraper run finished");
	}
	
	public void parseHtml( String pageUrl, String html) throws Exception{
		ProviderDAO providerDAO  = new ProviderDAO(dbCon);
		Document page = Jsoup.parse(html,pageUrl);
		
		Element resultWrapper = page.getElementsByAttributeValue("class", "global camp_listing").first();
		Elements results1 = resultWrapper.getElementsByAttributeValue("class", "camp logos_1");
		Elements results2 = resultWrapper.getElementsByAttributeValue("class", "camp logos_2");
		Elements results = new Elements();
		if(results1 != null){
			results.addAll(results1);
		}
		if(results2 != null){
			results.addAll(results2);
		}
		if(results != null && results.size() > 0){
			String isExternalUrl = Constants.EXTERNAL_URL_YES;
			for (Element item : results) {
				Element name = item.getElementsByTag("h1").first();
				Element providerLink = name.getElementsByTag("a").first();
				String providerUrl = providerLink.attr("href");
				String providerName = providerLink.text().trim();
				
				//categories
				Element catElem = item.getElementsByClass("categories").first();
				String category = catElem.text().trim();
				ArrayList<String> categories = new ArrayList<String>();
				categories.add(category);
				
				Element locationElem = item.getElementsByClass("location").first();
				String locationName = locationElem.text().trim();
				String logoUrl = "";
				String programFor = "";
				String phoneNumber = "";
				String description = "";
				Elements imagesElem = item.getElementsByTag("img");
				for (Element img : imagesElem) {
					if(img.hasClass("accreditation")){
						String src = img.attr("src");
						if(src.contains("boy_and_girl")){
							programFor = Constants.PROGRAM_FOR_COED;
						}else if(src.contains("girl")){
							programFor = Constants.PROGRAM_FOR_GIRLS;
						}else if(src.contains("boy")){
							programFor = Constants.PROGRAM_FOR_BOYS;
						}
					}else {
						//logo img url
						logoUrl = img.absUrl("src");
					}
				}
				//phone
				Element contactWrapperElem = item.getElementsByClass("contact").first();
				if(contactWrapperElem != null){
					Element contactElem = contactWrapperElem.getElementsByTag("p").first();
					if(contactElem != null)
						phoneNumber = contactElem.text().trim();
					else {
						LOG.info("Phone number not found");
					}
				}
				//description
				Elements pElems = item.getElementsByTag("p");
				for (Element p : pElems) {
					if(p.attributes().size() == 0){
						description = p.text().trim();
						description = description.replaceFirst(" ...read more", "");
						break;
					}
				}
				HashMap<String, Object> resultMap = new HashMap<String, Object>();
				resultMap.put(Constants.KEY_SITE_NAME, SOURCE);
				resultMap.put(Constants.KEY_PAGE_URL, providerUrl);
				resultMap.put(Constants.KEY_WEBSITE, providerUrl);
				resultMap.put(Constants.KEY_LOGO_URL, logoUrl);
				resultMap.put(Constants.KEY_PROVIDER_NAME, providerName);
				resultMap.put(Constants.KEY_PHONE, phoneNumber);
				resultMap.put(Constants.KEY_DESCRIPTION, description);
				resultMap.put(Constants.KEY_PROGRAM_FOR, programFor);
				resultMap.put(Constants.KEY_LOCATION, locationName);
				resultMap.put(Constants.KEY_ACTIVITIES, categories);
				resultMap.put(Constants.KEY_IS_EXTERNAL_URL, isExternalUrl);
				HashMap<String, String> notesMap = new HashMap<String, String>();
				notesMap.put("source_url", pageUrl);
				JSONObject notes = new JSONObject(notesMap);
				resultMap.put(Constants.KEY_NOTES, notes.toString());
				LOG.info("result : "+new JSONObject(resultMap).toString());
				providerDAO.addProvider(resultMap);
			}
			
		}
	}
}
