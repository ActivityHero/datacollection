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

public class AisneScraper  implements IScraper {
	private static Logger LOG = LoggerFactory.getLogger(AisneScraper.class);
	public static String SITE_HOME_URL = "http://www.aisne.org/summer-programs.html";
    public static String SOURCE = "aisne";
    
    private DBConnection dbCon;
    
    public AisneScraper(){
		dbCon = new DBConnection(); 
	}
    
	
	public void run() {
		try { 
			LOG.info("AisneScraper run called");
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
		MainPageDAO htmlDAO = new MainPageDAO(dbCon);
		Document page = Jsoup.connect(SITE_HOME_URL).timeout(Constants.REQUEST_TIMEOUT).get();
		Element result = page.getElementById("results");
		if(result == null){
			LOG.info(SOURCE+" No results found.");
		}
		Elements results = result.getElementsByTag("h3");
		if(result != null){
			for (Element h3 : results) {
				Element link = h3.getElementsByTag("a").first();
				if(link == null)
					continue;
				String pageUrl = link.absUrl("href");
				if(pageUrl != null && !pageUrl.isEmpty()){
					String pageHtml = htmlDAO.getHtmlPage(pageUrl, false);
					if(pageHtml == null){
						ScraperUtil.sleep();
						LOG.info(SOURCE+ " details page url = "+pageUrl);
						page = Jsoup.connect(pageUrl).timeout(Constants.REQUEST_TIMEOUT).get();
						pageHtml = page.toString();
						htmlDAO.addHtmlPage(pageUrl, pageHtml, SOURCE);
					}
					
					parsePage(pageUrl, pageHtml);
				}
			}
		}
		
		LOG.info("AisneScraper run finished");
		
	}
	
	private void parsePage(String pageUrl, String html) throws Exception{
		ProviderDAO providerDAO  = new ProviderDAO(dbCon);
		Document page = Jsoup.parse(html, pageUrl);
		Element mainElement = page.getElementById("main");
		if(mainElement == null){
			LOG.info("No info found on page "+pageUrl);
		}else{
			Element intro = mainElement.getElementsByClass("intro").last();
			HashMap<String, Object> resultMap = new HashMap<String, Object>();
			resultMap.put(Constants.KEY_SITE_NAME, SOURCE);
			resultMap.put(Constants.KEY_PAGE_URL, pageUrl);
			if(intro != null){
				Element providerElem = intro.getElementsByTag("h4").first();
				if(providerElem != null){
					Element providerLink = providerElem.getElementsByTag("a").first();
					resultMap.put(Constants.KEY_PROVIDER_NAME, providerLink.text().trim());
				}
				
				Element descElem = intro.getElementsByTag("p").first();
				if(descElem != null){
					String desc = descElem.html().trim();
					resultMap.put(Constants.KEY_DESCRIPTION, desc);
				}
				Element locationElem = providerElem.nextElementSibling();
				String location = locationElem.text().trim();
				String[] locTokens = location.split(",");
				if(locTokens.length > 1){
					resultMap.put(Constants.KEY_CITY, locTokens[0].trim());
					resultMap.put(Constants.KEY_STATE, locTokens[1].trim());
				}
				
				Element infoElem = intro.getElementsByTag("ul").first();
				if(infoElem != null && infoElem.attr("class").equals("info")){
					Elements liElems = infoElem.getElementsByTag("li");
					for (Element li : liElems) {
						String text = li.text().trim();
						if(text.startsWith("Contact Name")){
							text = text.replaceFirst("Contact Name: ", "");
							resultMap.put(Constants.KEY_CONTACT_NAME, text);
						}else if(text.startsWith("Contact Phone")){
							text = text.replaceFirst("Contact Phone: ", "");
							String[] phones = text.split(",");
							ArrayList<String> phoneNumbers = new ArrayList<String>();
							for (String phone : phones) {
								phoneNumbers.add(phone.trim());
							}
							if(phoneNumbers.size() > 0){
								resultMap.put(Constants.KEY_PHONE, phoneNumbers);
								resultMap.put(Constants.KEY_PHONE, ScraperUtil.tabbedStrFromMap(resultMap, Constants.KEY_PHONE));
							}
						}else if(text.startsWith("Program Website")){
							text = text.replace("Program Website: ", "");
							resultMap.put(Constants.KEY_WEBSITE, text);
						}else if(text.startsWith("Contact Email")){
							text = text.replaceFirst("Contact Email: ", "");
							resultMap.put(Constants.KEY_EMAIL, text);
						}else if(text.startsWith("Program Type")){
							text = text.replaceFirst("Program Type: ", "");
							ArrayList<String> activities = new ArrayList<String>();
							activities.add(text);
							resultMap.put(Constants.KEY_ACTIVITIES, activities);
						}else if(text.startsWith("Residential/Day")){
							text = text.replaceAll("Residential/Day: ", "");
							resultMap.put(Constants.KEY_PROGRAM_TYPE, text);
						}else if(text.startsWith("Gender")){
							text = text.replaceFirst("Gender: ", "");
							if(text.equals("Girls"))
								resultMap.put(Constants.KEY_PROGRAM_FOR, Constants.PROGRAM_FOR_GIRLS);
							else if(text.equals("Boys"))
								resultMap.put(Constants.KEY_PROGRAM_FOR, Constants.PROGRAM_FOR_BOYS);
							else if(text.equals("Coed"))
								resultMap.put(Constants.KEY_PROGRAM_FOR, Constants.PROGRAM_FOR_COED);
						} else if(text.startsWith("Ages")){
							text = text.replaceFirst("Ages:", "").trim();
							if(text.isEmpty())
								continue;
							
							String[] ages = text.split(",");
							if(ages.length > 0){
								String fromAge = ages[0];
								String toAge = ages[ages.length-1];
								if(fromAge.equals("adult"))
									fromAge = "99";
								if(toAge.equals("adult"))
									toAge = "99";
								resultMap.put(Constants.KEY_FROM_AGE, fromAge);
								resultMap.put(Constants.KEY_TO_AGE, toAge);
							}
						}
					}
				}
			}
			
			LOG.info("result : "+new JSONObject(resultMap).toString());
			providerDAO.addProvider(resultMap);
		}
	}

}
