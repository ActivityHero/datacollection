package com.ah.scraper.scrapers;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVWriter;

import com.ah.scraper.common.Constants;
import com.ah.scraper.common.ScraperUtil;

public class YelloPagesScraper {
	private static Logger LOG = LoggerFactory.getLogger(YelloPagesScraper.class);
	public static String SITE_HOME_URL = "http://www.yellowpages.com/search?search_terms=churches&geo_location_terms=Palo%20Alto%2C%20CA";
    public static String SOURCE = "yellowpages";
    public static int currentRecord = 1;
    
    public static void main(String[] args) {  
    	try {
    		startScraping();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	public void run() {
		try { 
			LOG.info("YPScraper run called");
			//this.startScraping();
		} catch (Exception e) { 
			LOG.error("err",e);
		}
		
	}

	public static void startScraping() throws Exception {
		boolean hasMoreData = true;
		CSVWriter writer = new CSVWriter(new FileWriter("/Users/satya/Documents/churches.csv"));
		while (hasMoreData) {
			hasMoreData = false;
			int from_record = YelloPagesScraper.currentRecord;
			YelloPagesScraper.currentRecord += 1;
			
			String pageUrl =  SITE_HOME_URL + "&page="+from_record;
			System.out.println(pageUrl);
			
			//LOG.info(SOURCE+": Getting data from = "+pageUrl);
			ScraperUtil.sleep();
			
			Document page = Jsoup.connect(pageUrl)
					.timeout(Constants.REQUEST_TIMEOUT).get();
			Element resultWrapper = page.getElementById("main-content");
			if(resultWrapper == null){
				LOG.info(SOURCE+": results-wrapper not found");
				break;
			}
			
			Elements items = resultWrapper.getElementsByClass("v-card");
			
			if(items != null){
				hasMoreData = true;
				for (Element element : items) {
					Element h3 = element.getElementsByTag("h3").first();
					if(h3.hasClass("n")){
						String detailUrl = h3.getElementsByTag("a").first().absUrl("href").trim();
						if(detailUrl != null && !detailUrl.isEmpty()){
							parsePage(detailUrl, writer);
						}
					}
				}
			}
			writer.flush();
		}
	}
	
	private static void parsePage(String pageUrl, CSVWriter writer){
		try {
			
			System.out.println("details page url "+pageUrl);
			Document pageDetails = Jsoup.connect(pageUrl).timeout(Constants.REQUEST_TIMEOUT).get();
			Element mainContent = pageDetails.getElementById("main-content");
			if(mainContent != null){
				HashMap<String, String> record = new HashMap<String, String>();
				record.put("churchName", "");
				record.put("address", "");
				record.put("phone", "");
				record.put("city", "");
				record.put("state", "");
				record.put("zip", "");
				record.put("website", "");
				record.put("email", "");
				record.put("categories", "");
				ArrayList<String> result = new ArrayList<String>();
				Element businessCard = mainContent.getElementsByClass("business-card").first();
				if(businessCard != null){
					result.add(0, businessCard.getElementsByTag("h1").first().text().trim());
					Element contact = businessCard.getElementsByClass("contact").first();
					if(contact != null){
						Element address = contact.getElementsByClass("street-address").first();
						if(address != null){
							String adr = address.text().trim();
							result.add(1, adr.substring(0, adr.lastIndexOf(",")));
						}
						else
							result.add(1,"");
						Element phone = contact.getElementsByClass("phone").first();
						if(phone != null)
							result.add(2, phone.text().trim());
						else
							result.add(2,"");
						Element cityState = contact.getElementsByClass("city-state").first();
						if(cityState != null){
							String[] parts = cityState.text().trim().split(",");
							result.add(3, parts[0].trim());
							String[] locParts = parts[1].trim().split(" ");
							result.add(4, locParts[0].trim());
							result.add(5, locParts[1].trim());
						}else {
							result.add(3, "");
							result.add(4, "");
							result.add(5, "");
						}
					}else{
						result.add(1,"");
						result.add(2,"");
						result.add(3,"");
						result.add(4, "");
						result.add(5, "");
					}
					Element footer = businessCard.getElementsByTag("footer").first();
					if(footer != null){
						Element website = footer.getElementsByClass("custom-link").first();
						if(website != null){
							result.add(6, website.absUrl("href").trim());
						}else
							result.add(6, "");
						Element email = footer.getElementsByClass("email-business").first();
						if(email != null){
							result.add(7, email.absUrl("href").trim().replace("mailto:", ""));
						}else
							result.add(7, "");
					}else{
						result.add(6, "");
						result.add(7, "");
					}
				}else{
					result.add(1,"");
					result.add(2,"");
					result.add(3,"");
					result.add(4, "");
					result.add(5, "");
					result.add(6, "");
					result.add(7, "");
				}
				Element businessContent = mainContent.getElementsByClass("business-content").first();
				if(businessContent != null){
					Element categories = businessContent.getElementsByAttributeValue("class", "categories").get(1);
					if(categories != null){
						result.add(8, categories.text().trim());
					}else
						result.add(8, "");
				}else{
					result.add(8, "");
				}
				String[] detail = result.toArray(new String[] {});
				System.out.println(detail[0]);
				writer.writeNext(detail);
				writer.flush();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
