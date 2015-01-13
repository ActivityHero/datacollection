package com.ah.scraper.scrapers;

import java.util.HashMap;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ah.scraper.common.Constants;
import com.ah.scraper.common.DBConnection;
import com.ah.scraper.dao.ActivityDAO;
import com.ah.scraper.dao.MainPageDAO;
import com.ah.scraper.dao.ProviderDAO;

public class ActivityRocketScraper implements IScraper {
	
	private static Logger LOG = LoggerFactory.getLogger(AisneScraper.class);
	public static String SITE_HOME_URL = "https://www.activityrocket.com/site_ajax_act_provider_search.php?frmSearchType=provider&frmLocationSearch=&frmSortBy=&alpha=all&page=";
   	public static String SOURCE = "activityrocket";
    
    private DBConnection dbCon;

    public ActivityRocketScraper(){
    	dbCon = new DBConnection(); 
    }
	public void run() {
		try { 
			LOG.info("ActivityRocketScraper run called");
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
		ProviderDAO providerDAO = new ProviderDAO(dbCon);
		int i = 1;
		while(true){
			LOG.info("we are at page : "+i);
			Document page = Jsoup.connect(SITE_HOME_URL+i).timeout(Constants.REQUEST_TIMEOUT).header("User-Agent", Constants.USER_AGENT).get();
			if(page != null){
				Element providerElem = page.getElementsByClass("provider-directory").first();
				if(providerElem != null){
					if(providerElem.text().contains("There are no results at this time.")){
						LOG.info("ActivityRocketScraper finished");
						break;
					}
					Elements boxElem = providerElem.getElementsByClass("box");
					if(boxElem != null){
						for (Element box : boxElem) {
							Element headingElem = box.getElementsByTag("h2").first();
							Element aElem = headingElem.getElementsByTag("a").first();
							String aLink = aElem.absUrl("href").trim();
							if(aLink.contains("https://www.activityrocket.com")){
								if(!providerDAO.isRecordExist(aLink)){
									String pageHtml = htmlDAO.getHtmlPage(aLink, false);
									if(pageHtml == null){
										LOG.info(SOURCE + " details page url = " + aLink);
										page = Jsoup.connect(aLink).timeout(Constants.REQUEST_TIMEOUT).header("User-Agent", Constants.USER_AGENT).get();
										pageHtml = page.toString();
										htmlDAO.addHtmlPage(aLink, pageHtml, SOURCE);
									}
									parsePage(aLink, pageHtml);
								}
							}
						}
					}
					
				}
			}
			i++;
		}
		
	}
	
	private void parsePage(String link, String html) throws Exception{
		LOG.info("parse data of url : " + link);
		Document page = Jsoup.parse(html, link);
		if(page != null){
			ProviderDAO providerDAO = new ProviderDAO(dbCon);
			HashMap<String, Object> resultMap = new HashMap<String, Object>();
			resultMap.put(Constants.KEY_SITE_NAME, SOURCE);
			resultMap.put(Constants.KEY_PAGE_URL, link);
			
			Element providerElem = page.getElementsByClass("provider-info").first();
			if(providerElem != null){
				Element titleElem = providerElem.getElementsByClass("title").first();
				if(titleElem != null){
					Element headElem = titleElem.getElementsByTag("h1").first();
					String proName = headElem.text().trim();
					resultMap.put(Constants.KEY_PROVIDER_NAME, proName);
					Element logoElem = titleElem.getElementsByTag("img").first();
					if(logoElem != null){
						String logosrc = logoElem.absUrl("src").trim();
						resultMap.put(Constants.KEY_LOGO_URL, logosrc);
					}
				} //title finish
				
				Element articleElem =  providerElem.getElementsByTag("article").first();
				Element weblinkELem = articleElem.getElementsByTag("a").first();
				if((weblinkELem != null) && (weblinkELem.hasAttr("data-url"))){
					String website = weblinkELem.attr("data-url");
					resultMap.put(Constants.KEY_WEBSITE, website);
				}
				Element addElem = articleElem.getElementsByTag("address").first();
				if(addElem != null){
					String addhtml = addElem.html().trim();
					String[] values = addhtml.split("<br>");
					if(values.length>2){
						if(!values[0].startsWith("Multi-City Business")){
							resultMap.put(Constants.KEY_STREET_ADDRESS, values[0]);
							String[] add = values[1].split("&nbsp;");
							resultMap.put(Constants.KEY_ZIP_CODE, add[1]);
							if(add[0].contains(",")){
								String[] city = add[0].split(", ");
								resultMap.put(Constants.KEY_CITY, city[0].trim());
								resultMap.put(Constants.KEY_STATE, city[1].trim());
							}else{
								resultMap.put(Constants.KEY_STATE, add[0].trim());
							}
							
						}
					}else if(values.length == 2){
						if(!values[0].startsWith("Multi-City Business")){
							if(values[0].contains(",")){
								String[] add = values[0].split("&nbsp;");
								resultMap.put(Constants.KEY_ZIP_CODE, add[1]);
								String[] city = add[0].split(", ");
								resultMap.put(Constants.KEY_CITY, city[0].trim());
								resultMap.put(Constants.KEY_STATE, city[1].trim());
							}
							
						}
					}else{
						if(!values[0].startsWith("Multi-City Business")){
							values = addhtml.split("<p>");
							if(values[0].contains(",")){
								String[] city = values[0].split(",");
								resultMap.put(Constants.KEY_CITY, city[0]);
								resultMap.put(Constants.KEY_STATE, city[1]);
							}
						}
					}   // address finshed
					
					Element phoneElem = addElem.getElementsByTag("a").first();
					if(phoneElem != null){
						String phone = phoneElem.attr("data-phone").trim();
						resultMap.put(Constants.KEY_PHONE, phone);
					}
				}
				Elements pElem = articleElem.getElementsByTag("p");
				if(pElem != null){
					String proDesc = "";
					for (Element p : pElem) {
						String pData = p.text().trim();
						if(!(pData.equals("Visit Website") || pData.equals("View Phone Number") || pData.equals("&nbsp;"))){
							proDesc = proDesc+" "+pData;
						}
					}
					resultMap.put(Constants.KEY_DESCRIPTION, proDesc);                   
				}
				
				// activities started
				Element actElem = providerElem.getElementById("tab1");
				if(actElem != null){
					if(!actElem.text().startsWith("No activities found")){
						resultMap.put(Constants.KEY_HAS_CLASS, "Y");
					}
				}
				Element campElem = providerElem.getElementById("tab2");
				if(campElem != null){
					if(!campElem.text().startsWith("No camps found")){
						resultMap.put(Constants.KEY_HAS_CAMP, "Y");
					}
					
				}
				LOG.info("provider detail : " + resultMap.toString());
				int provider_id = providerDAO.addProvider(resultMap);
				
				getActivitiesDetails(actElem, "Activities", provider_id, link );
				getActivitiesDetails(campElem, "Camps", provider_id, link);
				
			}
		}
		
	}
	
	private void getActivitiesDetails(Element actElem, String type, int provider_id, String link) throws Exception{
		ActivityDAO activityDAO = new ActivityDAO(dbCon);
		if(actElem != null){
			if(actElem.text().startsWith("No camps found")){
				LOG.info("No " + type + " Found At : " + link); 
			}else if(actElem.text().startsWith("No activities found")){
				LOG.info("No " + type + " Found At : " + link);
			}else{
				Elements actBoxElem = actElem.getElementsByClass("activity-box");
				if(actBoxElem != null){
					for (Element actbox : actBoxElem) {
						HashMap<String, Object> map = new HashMap<String, Object>();
						map.put(Constants.KEY_PROVIDER_ID, provider_id);
						Element actnameElem = actbox.getElementsByTag("h2").first();
						if(actnameElem != null){
							String actName = actnameElem.text().trim();
							map.put(Constants.KEY_ACTIVITY_NAME, actName);
						}
						Element progForElem = null;
						progForElem = actbox.getElementsByClass("girls-only").first();
						if(progForElem != null){
							String gender = progForElem.text().trim();
							if(gender.equals("girls only")){
								map.put(Constants.KEY_PROGRAM_FOR, Constants.PROGRAM_FOR_GIRLS);
							}else if(gender.equals("boys only")){
								map.put(Constants.KEY_PROGRAM_FOR, Constants.PROGRAM_FOR_BOYS);
							}
						}
						
						Elements actDescElem = actbox.getElementsByTag("div");
						for (Element element : actDescElem) {
							if(element.hasClass("frame") && element.hasAttr("id")){
								String actDesc = element.text().trim();
								if(actDesc.contains("Read More...")){
									actDesc = actDesc.replace("Read More...", "");
								}
								
								map.put(Constants.KEY_DESCRIPTION, actDesc);
							}
						} //activity description ended
						
						Element priceElem = actbox.getElementsByTag("strong").first();
						if(priceElem != null){
							String priceText = priceElem.text().trim();
							if(!(priceText.contains("Call Provider") || priceText.contains("Free"))){
								if(priceText.contains("—")){
									String[] values = priceText.split("—");
									map.put(Constants.KEY_FROM_PRICE, values[0].replace("$", ""));
									map.put(Constants.KEY_TO_PRICE, values[1].replace("$", ""));
								}else{
									map.put(Constants.KEY_FROM_PRICE, priceText.replace("$", ""));
								}
							}
						}
						
						Element ageElem = actbox.getElementsByClass("age").first();
						if(ageElem != null){
							String age = ageElem.text().trim();
							int fromAge = -1;
							int toAge = -1;
							if(age.contains(" - ")){
								String[] values = age.split(" - ");
								String from_age_str = values[0].trim();
								if(from_age_str.contains(" ")){
									String[] from = from_age_str.split(" ");
									fromAge = Integer.valueOf(from[0]);
								}else{
									fromAge = (int)(Float.valueOf(from_age_str)*12);
								}
								map.put(Constants.KEY_FROM_AGE, String.valueOf(fromAge));
							//to_age	
								String to_age_str = values[1].trim();
								if(to_age_str.contains(" ")){
									String[] from = to_age_str.split(" ");
									toAge = Integer.valueOf(from[0]);
								}else{
									toAge = (int)(Float.valueOf(to_age_str)*12);
								}
								map.put(Constants.KEY_TO_AGE,  String.valueOf(toAge));
							}else{
								String from_age_str = age.trim();
								if(from_age_str.contains(" ")){
									String[] from = from_age_str.split(" ");
									fromAge = Integer.valueOf(from[0]);
								}else{
									fromAge = (int)(Float.valueOf(from_age_str)*12);
								}
								map.put(Constants.KEY_FROM_AGE, String.valueOf(fromAge));
							}
						}
						
						if(type.equals("Camps")){
							map.put(Constants.KEY_ACTIVITY_TYPE, "Camp");
						}else if(type.equals("Activities")){
							map.put(Constants.KEY_ACTIVITY_TYPE, "Class");
						}
						System.out.println(map.toString());
						LOG.info("Activity detail : "+map.toString());
						activityDAO.addActivity(map);
					}
				}
			}
		}
		
	}
	

}
