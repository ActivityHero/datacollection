package com.ah.scraper.scrapers;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
import com.ah.scraper.dao.ZipProcessedDAO;



public class SavvySourceScraper implements IScraper {
	private static Logger LOG = LoggerFactory.getLogger(SavvySourceScraper.class);
	private String SITE_HOME_URL = "http://www.savvysource.com/camps-classes/search/page/"; 
	public static String SOURCE = "savvysource";
	private DBConnection dbCon;
	
	private int minThreads = 1;
	private int maxThreads = 20;
	private int queueSize = 100; 
	ThreadPoolExecutor executorService;	
	public SavvySourceScraper(){
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
	public void run() {
		try {
			LOG.info("Scraper run called");
			this.startScraping();
		} catch (Exception e) {
			LOG.error("err",e);
		}
		// close connections
		try {
			dbCon.close();
		} catch (Exception e) {
			LOG.error("err",e);
		}
	}
	public void startScraping() throws Exception {
		// create DAOs
		ZipProcessedDAO zipDAO = new ZipProcessedDAO(dbCon);
		LOG.info("SavvySourceScraper launched");
		ArrayList<String> zips = zipDAO.getUnprocessedZip(SOURCE, queueSize);

		while (zips.size() > 0) {
			for (String zip : zips) {
				executorService.submit(new ScraperThread(zip));
			}
			do {
				// just wait
			} while(executorService.getActiveCount() > minThreads * 3);
			LOG.info("------------------------------Going to get more zips now--------------------------");
			zips = zipDAO.getUnprocessedZip(SOURCE, queueSize - minThreads *3);
		}
		LOG.info("SavvySourceScraper finished");

	}
	
	private class ScraperThread implements Runnable {
		String zip;
		DBConnection dbConT = new DBConnection();
		ZipProcessedDAO zipDAO = new ZipProcessedDAO(dbConT);
		
		
		public ScraperThread(String zip) {
			this.zip = zip;
		}
		
		public void run() {
			try{
				MainPageDAO htmlDAO = new MainPageDAO(dbConT);
				ProviderDAO providerDAO = new ProviderDAO(dbConT);
				int i = 1;
				int pageLimit = 2;
				while(i < pageLimit){
					
					LOG.info("we are at page: " + i+ " for zip: "+zip);
					Document page = Jsoup.connect(SITE_HOME_URL+i).data("miles","100").data("zipcode", String.valueOf(zip))
					.data("formSubmited","1").userAgent(Constants.USER_AGENT)
					.timeout(Constants.REQUEST_TIMEOUT).post();
					Element pageEndElem = page.getElementById("pagination_box");
					if(pageEndElem != null){
						Element limit = pageEndElem.getElementsByTag("a").last();
						pageLimit = Integer.parseInt(limit.text().trim());
						LOG.info("Remaining pages for this zip : "+zip+" to be processed : "+ (pageLimit-i));
					}/*else{
						break;
						//pageLimit = 0;
					}*/
					
					Element mainElem = page.getElementById("nss_left");
					if(mainElem != null){
						Element noResult = mainElem.getElementsByClass("no_result_box").first();
						if(noResult != null){
							LOG.info("No data found at page: " +i+" for zip :" + zip);
						//	ScraperUtil.log("Scraping finished for zip : "+zip);
						//	continue;
							break;
						}
						Elements campElem = mainElem.getElementsByTag("h2");
						for (Element heading : campElem) {
							String headingText = heading.text().trim();
							if(!headingText.isEmpty() && headingText.startsWith("Spotlight Camps")){
								Element contentElem = mainElem.getElementsByClass("mb_30").first();
								Elements aElems = contentElem.getElementsByTag("a");
								for (Element element : aElems) {
									if(element.hasClass("main")){
										String link = element.absUrl("href");
										if(!providerDAO.isRecordExist(link)){
											String pageHtml;
											pageHtml = htmlDAO.getHtmlPage(link, false);
											if(pageHtml == null){
												ScraperUtil.sleep();
												ScraperUtil.log(SOURCE+ " details page url = "+link);
												page = Jsoup.connect(link).timeout(Constants.REQUEST_TIMEOUT).get();
												pageHtml = page.toString();
												htmlDAO.addHtmlPage(link, pageHtml, SOURCE);
											}
												parsePage(link, pageHtml);
										}
									}
								}
						//		processResults(aElems, htmlDAO);
							} //spotlight links are over
							if(!headingText.isEmpty() && headingText.startsWith("Featured Camps")){
								Element featuredElem = mainElem.getElementsByClass("cac_list2").first();
								if(featuredElem != null){
									Elements aElems = featuredElem.getElementsByTag("a");
									processResults(aElems, htmlDAO, providerDAO);
								}
							}
							if(!headingText.isEmpty() && headingText.startsWith("Other Camps")){
								Element otherCampElem = mainElem.getElementsByClass("cac_list").first();
								if(otherCampElem != null){
									Elements aElems = otherCampElem.getElementsByTag("a");
									processResults(aElems, htmlDAO, providerDAO);
								}
							} // other camp links are over
						}
					}
					i++;
				}
				LOG.info("scraping finish for zip:" + zip );
				zipDAO.markProcessed(zip, SOURCE);
				
			}catch(Exception e) {
				LOG.error("err",e);
			}
			// close connections
			try {
				dbConT.close();
			} catch (Exception e) {
				LOG.error("err",e);
			}
		} // run method -finish
		
		private void processResults(Elements aElems, MainPageDAO htmlDAO, ProviderDAO providerDAO){
			if(aElems != null){
				for (Element element : aElems) {
					if(element.hasText()){
						String link = element.absUrl("href");
						if(!providerDAO.isRecordExist(link)){
							String pageHtml;
							try {
								pageHtml = htmlDAO.getHtmlPage(link, false);
								if(pageHtml == null){
									ScraperUtil.sleep();
									ScraperUtil.log(SOURCE+ " details page url = "+link);
									Document page = Jsoup.connect(link).timeout(Constants.REQUEST_TIMEOUT).get();
									pageHtml = page.toString();
									htmlDAO.addHtmlPage(link, pageHtml, SOURCE);
								}
								parsePage(link, pageHtml);
							} catch (SQLException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							} catch (Exception e) {
								e.printStackTrace();
							}
							
						}
					}
				}
			}
		}
		
		private void parsePage(String link, String html) throws Exception{
			ProviderDAO providerDAO = new ProviderDAO(dbCon);
			HashMap<String, Object> resultMap = new HashMap<String, Object>();
			resultMap.put(Constants.KEY_SITE_NAME, SOURCE);
			resultMap.put(Constants.KEY_PAGE_URL, link);
			resultMap.put(Constants.KEY_IS_EXTERNAL_URL, "N");
			Document page = Jsoup.parse(html);
			if(page != null){
				Element dataElem = page.getElementById("nss_left") ;
				if(dataElem != null){
					Element noResult = dataElem.getElementsByClass("no_result_box").first();
					if(noResult != null){
						LOG.info("No data found at page: " + link);
						return;
					}
					Element heading = dataElem.getElementById("band");
					if(heading != null){
						String headingText = heading.text().trim();
						if(headingText.contains("Camp Information")){
							Elements listbox = dataElem.getElementsByClass("list_box");
							boolean cType = false;
							boolean cSubject = false;
							boolean cAge = false;
							for (Element element : listbox) {
								//System.out.println(element.children().size());
								Elements elem = element.children();
								for (Element element2 : elem) {
									if(element2.text().equals("Camp Type:")){
										cType = true; cSubject = false; cAge = false;
										continue;
									}else if(element2.text().equals("Camp Subject:")){
										cType = false; cSubject = true; cAge = false;
										continue;
									}else if(element2.text().equals("Camp Age:")){
										cType = false; cSubject = false; cAge = true;
										continue;
									}else if(element2.text().equals("Camp Season:")){
										cType = false; cSubject = false; cAge = false;
										continue;
									}
									if(cType){
										String campdata = element2.text().trim();
										String programType="";
										if(campdata.contains("Day Camp")){
											programType = "Day Camp";
										}else if(campdata.contains("Sleepaway Camp")){
											programType = "Sleepaway Camp";
										}
										String programFor ="";
										if(campdata.contains("Girls Camp")){
											programFor = Constants.PROGRAM_FOR_GIRLS;
										}
										if(campdata.contains("Boys Camp")){
											programFor = Constants.PROGRAM_FOR_BOYS;
										}
										if(campdata.contains("Girls Camp") && campdata.contains("Boys Camp")){
											programFor = Constants.PROGRAM_FOR_COED;
										}
										resultMap.put(Constants.KEY_HAS_CAMP, "Y");
										resultMap.put(Constants.KEY_HAS_CLASS, "N");
										resultMap.put(Constants.KEY_HAS_BIRTHDAY_PARTY, "N");
										resultMap.put(Constants.KEY_PROGRAM_TYPE, programType);
										if(programFor != ""){
											resultMap.put(Constants.KEY_PROGRAM_FOR, programFor);
										}
									}
									if(cAge){
										ArrayList<Integer> age = new ArrayList<Integer>();
										Elements liElem = element2.getElementsByTag("li");
										boolean x;
										if(liElem.size()>1){
										
											for (Element li : liElem) {
												String data = li.text().trim();
												String[] values = data.split(" ");
												if(values[0].equals("<") || values[0].equals("&lt")){
													x=checkIfNum(values[1]);
													if(x){
														age.add(Integer.parseInt(values[1]));
													}
												}else{
													x = checkIfNum(values[0]);
													if(x){
														age.add(Integer.parseInt(values[0]));
													}
												}
											}
										//	System.out.println(age.toString());
											if(age.size()>0){
												int min = Collections.min(age);
												int max = Collections.max(age);
												resultMap.put(Constants.KEY_FROM_AGE, String.valueOf(min));
												resultMap.put(Constants.KEY_TO_AGE, String.valueOf(max));
											}
										}else{
											String data = liElem.text().trim();
											String[] values = data.split(" ");
											if(values[0].equals("<") || values[0].equals("&lt")){
												x=checkIfNum(values[1]);
												if(x){
													resultMap.put(Constants.KEY_FROM_AGE, values[1]);
												}else{
													x = checkIfNum(values[0]);
													if(x)
														resultMap.put(Constants.KEY_FROM_AGE, values[0]);
												}
											}
										}
									//	System.out.println(liElem);
									
									}
									if(cSubject){
										Elements liElem = element2.getElementsByTag("li");
										String activityType="";
										if(liElem.size()>1){
											for (Element li : liElem) {
												activityType += li.text().trim()+"\t";
												if(activityType.length()>=70){break;}
											}
										}else{
											activityType = liElem.text().trim();
										}
										resultMap.put(Constants.KEY_ACTIVITY_TYPE, activityType);
									}
								}
							}
						}
					}
				}
				Element campDetailElem = page.getElementsByClass("cdetail_right").first();
				if(campDetailElem != null){
					dataElem = campDetailElem.getElementsByTag("h1").first();
					if(dataElem != null){
						String providerName = dataElem.text().trim();
						resultMap.put(Constants.KEY_PROVIDER_NAME, providerName);
					}
					dataElem = campDetailElem.getElementsByClass("address").first();
					if(dataElem != null){
						Elements address = dataElem.getElementsByTag("span");
						for (Element element : address) {
							String s = element.attr("itemprop");
							if(s.equals("street-address"))
								resultMap.put(Constants.KEY_STREET_ADDRESS, element.text().trim());
							if(s.equals("locality")){
								String city = element.text().trim();
								if(city.length()>0){
									city = city.substring(0,city.length()-1);
									resultMap.put(Constants.KEY_CITY, city);
								}
							}
							if(s.equals("region"))
								resultMap.put(Constants.KEY_STATE, element.text().trim());
							if(s.equals("postal-code"))
								resultMap.put(Constants.KEY_ZIP_CODE, element.text().trim());
						
						}
					}
					dataElem = campDetailElem.getElementsByClass("od").first();
					if(dataElem != null){
						Element phoneElem = dataElem.getElementsByTag("p").first();
						if( phoneElem != null){
							String[] values = phoneElem.text().trim().split(": ");
							resultMap.put(Constants.KEY_PHONE, values[1]);
							
						}
						Element webElem = dataElem.getElementsByTag("p").last();
						if( webElem != null){
							Element elem = webElem.getElementsByTag("a").first();
							if(elem != null){
								String website = elem.absUrl("href").trim();
								resultMap.put(Constants.KEY_WEBSITE, website);
							}else{
								return;
							}
						}
					}
				}
				Element pElem = campDetailElem.getElementsByTag("p").last();
				if(pElem != null){
					resultMap.put(Constants.KEY_DESCRIPTION, pElem.text().trim());
				}
				// contact info i.e. cdetail_right -finish.
				Element cmpImg = page.getElementsByClass("cdetail_left").first();
				if(cmpImg != null){
					Elements aElem = cmpImg.getElementsByTag("a");
					ArrayList<String> photoUrls = new ArrayList<String>();
					for (Element element : aElem) {
						Element img = element.getElementsByTag("img").first();
						String photourl = img.absUrl("src").trim();
						photoUrls.add(photourl);
						//System.out.println(img);
					}
					resultMap.put(Constants.KEY_PHOTO_URL, photoUrls);
				} // images for detail page -finish
				

			}// page -finish
			System.out.println(resultMap.toString());
			LOG.info("result : {}",resultMap.toString());
			
			providerDAO.addProvider(resultMap);
		}// parsePage method -finish
		
		private boolean checkIfNum(String a){
			try{
				Integer.parseInt(a);
		        return true;
		    } catch (NumberFormatException nfe) {
		        return false;
		    }
			
		}
		
	}// ScraperThread class -finish
}// SavvySourceSCraper class -finish