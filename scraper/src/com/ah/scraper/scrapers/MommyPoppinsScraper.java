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
import com.ah.scraper.common.ScraperUtil;
import com.ah.scraper.dao.MainPageDAO;
import com.ah.scraper.dao.ProviderDAO;

public class MommyPoppinsScraper implements IScraper {
	private static Logger LOG = LoggerFactory.getLogger(MommyPoppinsScraper.class);
	public static String SITE_HOME_URL = "http://la.mommypoppins.com/directory?ages=All&type=All&area=All&keys=&page=0%2C";
	    public static String SOURCE = "mommypoppins";
		private DBConnection dbCon;
		
		public MommyPoppinsScraper(){
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
		MainPageDAO htmlDAO = new MainPageDAO(dbCon);
		LOG.info("MommyPoppinsScraper launched");
		
		int pageNum = 0;
		while(true){
			Document page = Jsoup.connect(SITE_HOME_URL+pageNum).timeout(Constants.REQUEST_TIMEOUT).get();
			LOG.info("parsing at page : "+(pageNum+1));
			Element emptyCheck = page.getElementsByClass("view-empty").first();
			if(emptyCheck == null){
				String imgsrc="";
				Elements rowsElem = page.getElementsByClass("views-row");
				for (Element element : rowsElem){
					Element imgElem = element.getElementsByClass("thumbnail").first();
					if(imgElem != null){
						Element img = imgElem.getElementsByTag("img").first();
						imgsrc = img.absUrl("src").trim();
					}
					Element titleElem = element.getElementsByClass("title").first();
					if(titleElem.hasClass("title")){
						Element hrefElem = element.getElementsByTag("a").first();
						String newPageUrl =  hrefElem.absUrl("href").trim();
						if(newPageUrl != null && !newPageUrl.isEmpty()){
							String pageHtml = htmlDAO.getHtmlPage(newPageUrl, false);
							if(pageHtml == null){
								LOG.info(SOURCE+ " details page url = "+newPageUrl);
								page = Jsoup.connect(newPageUrl).timeout(Constants.REQUEST_TIMEOUT).get();
								pageHtml = page.toString();
								htmlDAO.addHtmlPage(newPageUrl, pageHtml, SOURCE);
							}
							LOG.info("going to get data from : "+newPageUrl);
							parsePage(newPageUrl, pageHtml, imgsrc);
						}
					}
				}
				pageNum++;
			}else{
				LOG.info(SOURCE+" Scraper is finished");
				break;
			}
			
		}
	
	}
		
	private void parsePage(String pageUrl, String html, String imgsrc) throws Exception{
		ProviderDAO providerDAO  = new ProviderDAO(dbCon);
		HashMap<String, Object> resultMap = new HashMap<String,Object>();
		resultMap.put(Constants.KEY_SITE_NAME, SOURCE);
		resultMap.put(Constants.KEY_PAGE_URL, pageUrl);
		
		if(!pageUrl.startsWith("http://la.mommypoppins.com")){
			resultMap.put(Constants.KEY_IS_EXTERNAL_URL, "Y");
		}
		Document page = Jsoup.parse(html, pageUrl);
		Element mainElement = page.getElementById("main");
		if(mainElement == null){
			LOG.info("No info found on page "+pageUrl);
		}else{
			String providerName = mainElement.getElementsByTag("h1").first().text().trim();
			resultMap.put(Constants.KEY_PROVIDER_NAME, providerName);
			
			Elements pElems = mainElement.getElementsByTag("p");
			if(pElems != null){
				String description="";
				if(pElems.size()>1){
					for (Element element : pElems) {
						description += element.text().trim()+" ";
					}
				}else{
					description = pElems.text().trim();
				}
				resultMap.put(Constants.KEY_DESCRIPTION, description);
			}
			String data = "";
			Elements catElems = mainElement.getElementsByClass("cats");
			for (Element element : catElems) {
				data = element.text().trim();
				Elements aElem = element.getElementsByTag("a");
				String[] values = data.split(":");
				if(values.length>0){
					if(values[0].equals("Type")){ 
						if(values[1].contains("Camp") || values[1].contains("Camps")){
							resultMap.put(Constants.KEY_HAS_CAMP, "Y");
							resultMap.put(Constants.KEY_HAS_CLASS, "N");
							resultMap.put(Constants.KEY_HAS_BIRTHDAY_PARTY, "N");
						}else if(values[1].contains("Class") || values[1].contains("Classes")){
							resultMap.put(Constants.KEY_HAS_CAMP, "N");
							resultMap.put(Constants.KEY_HAS_CLASS, "Y");
							resultMap.put(Constants.KEY_HAS_BIRTHDAY_PARTY, "N");
						}
					}else if(values[0].equals("Ages")){
						/*String[] ageData = values[1].split(", ");
						if((ageData.length==1) && ageData[0].contains("Baby")){
							fromAge = 0;
							toAge = 1;
						}else if((ageData.length==1) && ageData[0].contains("Preschoolers")){
							fromAge = 3;
							toAge = 5;
						}else if((ageData.length==1) && ageData[0].contains("Kids")){
							fromAge = 6;
							toAge = 12;
						}else if((ageData.length==1) && (ageData[0].contains("Tweens & Teens") || ageData[0].contains("Teens")) ){
							fromAge = 3;
							toAge = 5;
						}
*/							
						
					}else if(values[0].equals("Email")){
						if(values.length==2){
							resultMap.put(Constants.KEY_EMAIL, values[1].trim());
						}
					}else if(values[0].equals("Website")){
						for (Element element2 : aElem) {
							String hrefText = element2.attr("href");
							String targetAttr = element2.attr("target");
							if(targetAttr.startsWith("_blank")){
								resultMap.put(Constants.KEY_WEBSITE, hrefText);
							}
						}
					}
				}
			}
			
			Element dataElem = mainElement.getElementsByClass("street-address").first();
			if(dataElem != null){
				String street = dataElem.text().trim();
				resultMap.put(Constants.KEY_STREET_ADDRESS,street);
			}
			dataElem = mainElement.getElementsByClass("postal-code").first();
			if(dataElem != null){
				String zip = dataElem.text().trim();
				resultMap.put(Constants.KEY_ZIP_CODE,zip);
			}
			dataElem = mainElement.getElementsByClass("locality").first();
			if(dataElem != null){
				String city = dataElem.text().trim();
				resultMap.put(Constants.KEY_CITY,city);
			}
			dataElem = mainElement.getElementsByClass("region").first();
			if(dataElem != null){
				String state = dataElem.text().trim();
				resultMap.put(Constants.KEY_STATE,state);
			}
			dataElem = mainElement.getElementsByClass("value").first();
				if(dataElem != null){
					String contactno = dataElem.text().trim();
					resultMap.put(Constants.KEY_PHONE,contactno);
				}
			
			ArrayList<String> photoUrls = new ArrayList<String>();
			if(imgsrc != ""){
				photoUrls.add(imgsrc);
			}
			dataElem = mainElement.getElementById("field-slideshow-2-pager");
			if(dataElem != null){
				Elements imgElem = dataElem.getElementsByTag("img");
				
				for (Element img : imgElem) {
					 String photourl = img.absUrl("src").trim();
					 photoUrls.add(photourl);
				}
				resultMap.put(Constants.KEY_PHOTO_URL, photoUrls);
			}
			LOG.info("result : "+resultMap.toString());
			providerDAO.addProvider(resultMap);
			
		}
		
	}
	
}

	