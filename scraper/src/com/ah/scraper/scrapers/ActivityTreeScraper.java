package com.ah.scraper.scrapers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONObject;
import org.jsoup.Connection;
import org.jsoup.Connection.Method;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.FormElement;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ah.scraper.common.Constants;
import com.ah.scraper.common.DBConnection;
import com.ah.scraper.common.ScraperUtil;
import com.ah.scraper.dao.MainPageDAO;
import com.ah.scraper.dao.ProviderDAO;
import com.ah.scraper.dao.ZipProcessedDAO;

//TODO: Need to start threads over zip codes
public class ActivityTreeScraper  implements IScraper {
	private static Logger LOG = LoggerFactory.getLogger(ActivityTreeScraper.class);
	private String SITE_HOME_URL = "http://www.activitytree.com"; 
	public static String SOURCE = "activitytree";
	private DBConnection dbCon;
	
	private int minThreads = 1;
	private int maxThreads = 20;
	private int queueSize = 100; 
	ThreadPoolExecutor executorService;	
	public ActivityTreeScraper(){
		dbCon = new DBConnection(); 
		executorService =
				  new ThreadPoolExecutor(
				    minThreads, // core thread pool size
				    maxThreads, // maximum thread pool size
				    1, // time to wait before resizing pool
				    TimeUnit.MINUTES, 
				    new ArrayBlockingQueue<Runnable>(queueSize, true),
				    new ThreadPoolExecutor.CallerRunsPolicy());		
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
		//create DAOs
		ZipProcessedDAO zipDAO = new ZipProcessedDAO(dbCon); 
		
		LOG.info("ActivityTreeScraper launched");

		Connection.Response response = Jsoup
				.connect(SITE_HOME_URL)
				.timeout(Constants.REQUEST_TIMEOUT).method(Method.GET).execute();

		Document home = response.parse(); 
		ArrayList<String> zips = zipDAO.getUnprocessedZip(SOURCE, queueSize);
		
		while(zips.size() > 0){
			for(String zip: zips){
				executorService.submit(new ScraperThread(zip, home));
			}
			do{
				//just wait
			}while(executorService.getActiveCount()>minThreads*3); 
			LOG.info("---------------------------------------------------------Going to get more zips now--------------------------------");
			zips = zipDAO.getUnprocessedZip(SOURCE, queueSize - minThreads*3);
 		} 
		LOG.info("ActivityTreeScraper finished"); 
	}
	
	private class ScraperThread implements Runnable { 
		 
		String zip;
		Document home;
		DBConnection dbConT = new DBConnection(); 
		public ScraperThread(String zip, Document home) {
			this.zip = zip;
			this.home = home;
		}	

		public void run() {  
			
			try{
			//create DAOs
			ZipProcessedDAO zipDAO = new ZipProcessedDAO(dbConT);
			ProviderDAO providerDAO = new ProviderDAO(dbConT);
			
			LOG.info("Processing zip " + zip);
				
			boolean hasMoreData = true;
			FormElement searchForm = ((FormElement) home
						.getElementById("home_search_form"));
			Connection.Response response = Jsoup
					.connect(SITE_HOME_URL)
					.timeout(Constants.REQUEST_TIMEOUT).method(Method.GET).execute();
 		
			while (hasMoreData) {
				hasMoreData = false;
				Connection con = searchForm.submit();

				con.timeout(Constants.REQUEST_TIMEOUT).cookies(response.cookies())
							.data("page", "" + (1)).data("zip_code", zip)
							.data("activity_type", "all");

				ScraperUtil.sleep();
				response = con.method(Method.POST).execute();
				Document resultPage = response.parse();
				Elements rows = resultPage.getElementsByClass("listing_row");
				int resultCount = 0;

				for (Element row : rows) {

						if (!row.attr("class").contains("preferred") && !row.attr("class").contains("ad_spot")) {
							resultCount++;

							Element h5 = row.getElementsByTag("h5").first();
							Element link = h5.getElementsByTag("a").first();
							String absUrl = link.absUrl("href");
							
							if((Constants.SHOULD_OVERRIDE_RECORDS || !providerDAO.isRecordExist(absUrl))){
								parsePage(absUrl,link.baseUri());
							}
							
							
							hasMoreData = true;
						}
					}
					LOG.info("Record found: "+ resultCount);
					// Scrap for single page
					break;
				}
				  
				zipDAO.markProcessed(zip,SOURCE); 
			}
			catch(Exception e){
				LOG.error("err",e);
			}
			//close connections
			try{
				dbConT.close();
			}
			catch(Exception e){
				LOG.error("err",e);
			}
		}
		
		public void parsePage(String pageUrl, String baseUri) throws Exception {
			
			Document campDetails = null;
			MainPageDAO htmlDAO = new MainPageDAO(dbConT);
			ProviderDAO providerDAO = new ProviderDAO(dbConT);
			
			String pageHtml = htmlDAO.getHtmlPage(pageUrl, false);
			if(pageHtml == null){
				LOG.info("GET Camp: " + pageUrl);
				ScraperUtil.sleep();
				campDetails = Jsoup.connect(pageUrl).timeout(Constants.REQUEST_TIMEOUT).get();
				htmlDAO.addHtmlPage(pageUrl, campDetails.toString(),SOURCE);
			}else{
				campDetails = Jsoup.parse(pageHtml,baseUri);
				LOG.info("Page exists in db : " + pageUrl);
			}
			
			HashMap<String, Object> map = new HashMap<String, Object>();
			
			map.put(Constants.KEY_SITE_NAME, SOURCE);
			map.put(Constants.KEY_PAGE_URL, pageUrl);

			Element content = campDetails
					.getElementById("activity_listing_details_content");
			Element sideColumn = content
					.getElementById("listing_details_side_column");
			Element mainColumn = content
					.getElementById("listing_details_main_column");

			Elements logoHolders = sideColumn
					.getElementById("provider_logo_rating").getElementsByTag("img");

			if (logoHolders.size() > 0) {
				Element logo = logoHolders.get(0);
				String logoUrl = logo.absUrl("src");
				map.put(Constants.KEY_LOGO_URL, logoUrl);
			}

			Element slideShow = sideColumn
					.getElementById("listing_detail_photo_gallery");
			if (slideShow != null) {
				Elements images = slideShow.getElementsByTag("img");
				if (images != null && images.size() > 0) {
					ArrayList<String> photoUrls = new ArrayList<String>();
					for (Element photo : images) {
						String photoUrl = photo.absUrl("src").trim();
						if(photoUrl != null && !photoUrl.isEmpty())
							photoUrls.add(photoUrl);
					}
					if(photoUrls.size() > 0){
						map.put(Constants.KEY_PHOTO_URL, photoUrls);
					}
				}
			}

			if (mainColumn.children().size() > 1) {

				Element nameElement = mainColumn.children().first();
				String providerName = nameElement.text().trim();
				map.put(Constants.KEY_PROVIDER_NAME, providerName);

				int counter = 0;

				for (Element element : mainColumn.children().get(1).children()) {
					if (element.tagName().equals("div")
							|| element.tagName().equals("ul")) {
						if (counter == 0 && element.attr("class").isEmpty()) {
							getAddress(element, map);
						} else if (element.attr("class").equals("col-wrap left")
								&& counter == 1) {
							// website
							Element website = element.getElementsByTag("a").first();
							String webUrl = website.absUrl("href");
							map.put(Constants.KEY_WEBSITE, webUrl);

						} else if (element.attr("class").equals(
								"listing_details_list")) {
							Elements lis = element.getElementsByTag("li");
							for (Element element2 : lis) {
								Element h5 = element2.getElementsByTag("h5")
										.first();
								if(h5 == null)
									continue;
								
								Element p = element2.getElementsByTag("p").first();

								if (!map.containsKey(Constants.KEY_DESCRIPTION)
										&& h5.text().contains("Background")) {
									map.put(Constants.KEY_DESCRIPTION, p.text()
											.trim());

								} else if (h5.text().contains(
										"Kids Activities Offered")) {
									String text = p.text();
									String[] activityArr = text.trim()
											.split(",");
									if (activityArr.length > 0) {
										ArrayList<String> activities = new ArrayList<String>();
										for (int i = 0; i < activityArr.length; i++) {
											String act = activityArr[i];
											act = act.replaceAll("\u00A0", "");
											act = act.trim();
											activities.add(act);
										}
										map.put(Constants.KEY_ACTIVITIES, activities);
									}
								} else if (h5.text().contains("Activity Age Range")) {
									String[] ageRange = p.text().split(" to ");
									if (ageRange.length > 1) {
										String fromAge = ageRange[0];
										if (fromAge.contains("month")|| fromAge.contains("up")) {
											fromAge = "1";
										}
										String toAge = ageRange[1];
										if (toAge.contains("month")) {
											toAge = "1";
										}
										fromAge = fromAge.replace("months", "")
												.replace("month", "")
												.replace("years", "")
												.replace("year", "")
												.replace("old", "").trim();
										toAge = toAge.replace("months", "")
												.replace("month", "")
												.replace("years", "")
												.replace("year", "")
												.replace("old", "").trim();
										map.put(Constants.KEY_FROM_AGE, fromAge);
										map.put(Constants.KEY_TO_AGE, toAge);
									}
								}else if (h5.text().contains("Membership Fee")) {
									Pattern pattern = Pattern.compile("[$][\\d,.]+");
								    Matcher m = pattern.matcher(p.text());
								    ArrayList<String>prices = new ArrayList<String>();
								    while (m.find()) {
								    	prices.add(m.group());
								    }
								    Collections.sort(prices);
									if (prices.size() > 1) {
										String fromPrice = prices.get(0);
										String toPrice = prices.get(prices.size()-1);
										fromPrice = fromPrice.replace("$", "").replace(",", "").trim();
										toPrice = toPrice.replace("$", "").replace(",", "").trim();
										map.put(Constants.KEY_FROM_PRICE, fromPrice);
										map.put(Constants.KEY_TO_PRICE, toPrice);
									}
								}
							}

						}
						counter++;
					}

				}

				JSONObject json = new JSONObject(map);

				LOG.info("result : "+json.toString());
				providerDAO.addProvider(map);

			} else {
				LOG.info("NO data found");
			}

		}
	}
	public void getAddress(Element addressBox, HashMap<String, Object> map) {
		Element phone = addressBox.getElementById("phone_number");
		if (phone != null) {
			map.put(Constants.KEY_PHONE, phone.text().trim());
		}
		Elements spans = addressBox.getElementsByTag("span");
		for (Element span : spans) {
			if (span.attr("itemtype").equals("http://schema.org/PostalAddress")) {
				for (Element innerSpan : span.getElementsByTag("span")) {

					if (innerSpan.attr("temprop").equals("streetAddress")) {
						map.put(Constants.KEY_STREET_ADDRESS, innerSpan.text()
								.trim());

					} else if (innerSpan.attr("temprop").equals(
							"addressLocality")) {
						map.put(Constants.KEY_CITY, innerSpan.text().trim());

					} else if (innerSpan.attr("temprop")
							.equals("addressRegion")) {
						map.put(Constants.KEY_STATE, innerSpan.text().trim());

					} else if (innerSpan.attr("temprop").equals("postalCode")) {
						map.put(Constants.KEY_ZIP_CODE, innerSpan.text().trim());

					}
				}
			}
		}
	}

}
