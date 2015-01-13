package com.ah.scraper.scrapers;

import java.util.ArrayList;
import java.util.HashMap;


import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ah.scraper.common.Constants;
import com.ah.scraper.common.DBConnection;
import com.ah.scraper.dao.AddressDAO;
import com.ah.scraper.dao.MainPageDAO;
import com.ah.scraper.dao.ProviderDAO;

public class YmcaScraper implements IScraper {

	private static Logger LOG = LoggerFactory.getLogger(YmcaScraper.class);
	public static String SITE_HOME_URL = "http://www.ymca.net/find-your-y/?address=";
    public static String SOURCE = "ymca";
    
    private DBConnection dbCon;
    
    public YmcaScraper(){
		dbCon = new DBConnection(); 
	}
    
	
	public void run() {
		try { 
			LOG.info("YmcaScraper run called");
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
		AddressDAO addressDAO = new AddressDAO(dbCon);
		ArrayList<String> state_acr = addressDAO.getAllStateAcrynm();
		for (String state : state_acr) {
			MainPageDAO htmlDAO = new MainPageDAO(dbCon);
			ProviderDAO providerDAO = new ProviderDAO(dbCon);
			Document page = Jsoup.connect(SITE_HOME_URL + state).timeout(Constants.REQUEST_TIMEOUT).get();
			if(page != null){
				Element mainElem = page.getElementsByTag("td").first();
				if(mainElem != null){
					Elements divElem = mainElem.getElementsByTag("div");
					if(divElem != null){
						for (Element div : divElem){
							if(!div.hasClass("noprint")){
								Elements pElem = div.getElementsByTag("p");
								if(pElem != null){
									for (Element p : pElem) {
										Element aElem = p.getElementsByTag("a").first();
										if(aElem != null){
											String pageUrl = aElem.absUrl("href").trim();
											if(!pageUrl.isEmpty() && pageUrl != null && !providerDAO.isRecordExist(pageUrl)){
												String pageHtml = htmlDAO.getHtmlPage(pageUrl, false);
												if(pageHtml == null){
													System.out.println(SOURCE+ " details page url = "+pageUrl);
													page = Jsoup.connect(pageUrl).timeout(Constants.REQUEST_TIMEOUT).get();
													pageHtml = page.toString();
													htmlDAO.addHtmlPage(pageUrl, pageHtml, SOURCE);
												}
												parsePage(pageUrl, pageHtml);
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
	
	private void parsePage(String pageUrl, String html) throws Exception{
		ProviderDAO providerDAO  = new ProviderDAO(dbCon);
		Document page = Jsoup.parse(html, pageUrl);
		
		HashMap<String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put(Constants.KEY_SITE_NAME, SOURCE);
		resultMap.put(Constants.KEY_PAGE_URL, pageUrl);
		
		if(page != null){
			Element contentElem = page.getElementById("content");
			if(contentElem != null){
				Element titleElem = contentElem.getElementsByTag("h1").first();
				if(titleElem != null){
					String proName = titleElem.text().trim();
					if(!proName.isEmpty() && proName != null){
						resultMap.put(Constants.KEY_PROVIDER_NAME, proName);
					}
				}
				
				Elements divElem = contentElem.getElementsByTag("div");
				if(divElem != null){
					for (Element div : divElem) {
						if(div.hasAttr("style")){
							Elements pElem = div.getElementsByTag("p");
							if(pElem != null){
								for (Element p : pElem) {
									Element bElem = p.getElementsByTag("b").first();
									if(bElem != null && bElem.text().startsWith("Visit this Y's website now")){
										Element aElem = bElem.getElementsByTag("a").first();
										if(aElem != null){
											String website = aElem.absUrl("href").trim();
											if(!website.isEmpty() && website != null){
												resultMap.put(Constants.KEY_WEBSITE, website);
											}
										}
									}
									ArrayList<String> activities = new ArrayList<String>();
									String pText = p.text().trim();
									if(pText.startsWith("Programs:")){
										pText = pText.replace("Programs:", "").trim();
										String[] values = pText.split(",");
										if(values.length>0){
											for (String act : values) {
												activities.add(act);
											}
										}
									}
									if(activities.size()>0){
										resultMap.put(Constants.KEY_ACTIVITIES, activities);
									}
								}
								
								Element addElem = pElem.get(0);
								String street = addElem.text().trim();
								if(street.startsWith("Visit this Y's website now")){
									addElem = pElem.get(1);
									resultMap = getAddress(addElem, resultMap);
								}
								else{
									resultMap = getAddress(addElem, resultMap);								}
							}
						}
					}
				}
			}
			LOG.info("result : " + resultMap.toString());
			providerDAO.addProvider(resultMap);
		}
	}
	
	private HashMap<String, Object> getAddress(Element addElem, HashMap<String, Object> resultMap){
		String address = addElem.html().trim();
		String[] values = address.split("<br>");
		if(values.length == 3){
			resultMap.put(Constants.KEY_STREET_ADDRESS, values[0].trim());
			String[] state = values[1].split(",");
			if(state.length == 2){
				resultMap.put(Constants.KEY_CITY, state[0].trim());
				String[] zip = state[1].trim().split(" ");
				if(zip.length == 2){
					resultMap.put(Constants.KEY_STATE, zip[0].trim());
					resultMap.put(Constants.KEY_ZIP_CODE, zip[1].trim());
				}
			}
			if(values[2].startsWith("Phone:")){
				String phone = values[2].replace("Phone:", "").trim();
				if(!phone.isEmpty()){
					resultMap.put(Constants.KEY_PHONE, phone);
				}
			}
 		}else if(values.length == 4){
 			resultMap.put(Constants.KEY_STREET_ADDRESS, values[0].trim());
			String[] state = values[2].split(",");
			if(state.length == 2){
				resultMap.put(Constants.KEY_CITY, state[0].trim());
				String[] zip = state[1].trim().split(" ");
				if(zip.length == 2){
					resultMap.put(Constants.KEY_STATE, zip[0].trim());
					resultMap.put(Constants.KEY_ZIP_CODE, zip[1].trim());
				}
			}
			if(values[3].startsWith("Phone:")){
				String phone = values[3].replace("Phone:", "").trim();
				if(!phone.isEmpty()){
					resultMap.put(Constants.KEY_PHONE, phone);
				}
			}
		}
		return resultMap;
	}
}
