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
import com.ah.scraper.dao.MainPageDAO;
import com.ah.scraper.dao.ProviderDAO;

public class BayAreaParentKidsCampsScraper implements IScraper {

	private static Logger LOG = LoggerFactory.getLogger(BayAreaParentKidsCampsScraper.class);
	public static String SITE_HOME_URL = "http://bayareaparent.com/camp/search_results.php?start_date=Start%20Date&end_date=End%20Date&camp_type=0&keyword=&category_id=&activity_id=0&dist=50000&zip=&region_id=0&where=&gender=Coed&age=0&orderby=&results_per_page=40&advsearch=&screen=";
    public static String SOURCE = "bayareaparentkidscamps";
    
    private DBConnection dbCon;
    
    public BayAreaParentKidsCampsScraper(){
		dbCon = new DBConnection(); 
	}
    
	
	public void run() {
		try { 
			LOG.info("BayAreaParentKidsCampsScraper run called");
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
		int pageNum = 1;
		int i=1;
		while(i <= pageNum){
			LOG.info("we are at page : " + i );
			Document page = Jsoup.connect(SITE_HOME_URL + i).userAgent(Constants.USER_AGENT).timeout(Constants.REQUEST_TIMEOUT).get();
			Element pagination = page.getElementsByClass("pages").first();
			if(pagination != null){
				Element numElem = pagination.getElementsByTag("li").last();
				int num = Integer.parseInt(numElem.text().trim());
				pageNum = num;
			}
			
			Elements summaryElem = page.getElementsByClass("summary");
			if(summaryElem != null){
				for (Element summary : summaryElem) {
					Element titleElem = summary.getElementsByTag("h3").first();
					if(titleElem != null){
						Element aElem = titleElem.getElementsByTag("a").first();
						if(aElem != null){
							String pageUrl = aElem.absUrl("href").trim();
							if(pageUrl != null && !pageUrl.isEmpty()){
								if(!providerDAO.isRecordExist(pageUrl)){
									String pageHtml = htmlDAO.getHtmlPage(pageUrl, false);
									if(pageHtml == null){
										LOG.info(SOURCE+ " details page url = "+pageUrl);
										page = Jsoup.connect(pageUrl).userAgent(Constants.USER_AGENT).timeout(Constants.REQUEST_TIMEOUT).get();
										pageHtml = page.toString();
										htmlDAO.addHtmlPage(pageUrl, pageHtml, SOURCE);
									}
									parsePage(pageUrl, pageHtml);
								}
							}
						}else{
							parseData(summary);
						}
					}
				}
			}
			i++;
			
		}
		LOG.info("BayAreaParentKidsCampsScraper is finished.");
		
	}
	
	private void parseData(Element summary) throws Exception{
		
		ProviderDAO providerDAO = new ProviderDAO(dbCon);
		HashMap<String, Object> resultMap = new HashMap<String, Object>(); 
		Element titleElem = summary.getElementsByTag("h3").first();
		if(titleElem != null){
			String providerName = titleElem.text().trim();
			resultMap.put(Constants.KEY_PROVIDER_NAME, providerName);
		}
		
		Element col1Elem = summary.getElementsByClass("col-1").first();
		if(col1Elem != null){
			Elements liElem = col1Elem.getElementsByTag("li");
			if(liElem != null){
				String streetAdd = "";
				Element li1 = liElem.get(1);
				String li1Text = li1.text().trim();
				if(!li1Text.isEmpty()){
					streetAdd = li1Text;
				}else{
					streetAdd = liElem.get(0).text().trim();
				}
				if(!streetAdd.isEmpty()){
					resultMap.put(Constants.KEY_STREET_ADDRESS, streetAdd);
				} // street add finished
				
				Element li3 = liElem.get(2);
				String add = li3.text().trim();
				String[] values = add.split(",");
				/*resultMap.put(Constants.KEY_CITY, values[0].trim());
				resultMap.put(Constants.KEY_STATE, values[1].trim());*/
				if(values.length == 2 && values[0].trim().equals("United States")){
					resultMap.put(Constants.KEY_ZIP_CODE, values[1].trim());
				}else if(values.length == 3 && values[1].trim().equals("United States")){
					resultMap.put(Constants.KEY_STATE, values[0].trim());
					resultMap.put(Constants.KEY_ZIP_CODE, values[2].trim());
				}else if(values.length == 4){
					resultMap.put(Constants.KEY_CITY, values[0].trim());
					resultMap.put(Constants.KEY_STATE, values[1].trim());
					resultMap.put(Constants.KEY_ZIP_CODE, values[3].trim());
				}// address finished
				
				Element li4 = liElem.get(3);
				String website = fetchWebsite(li4);
				if(!(website == null)){
					resultMap.put(Constants.KEY_PAGE_URL, website);
					resultMap.put(Constants.KEY_WEBSITE, website);
					resultMap.put(Constants.KEY_IS_EXTERNAL_URL, "Y");
				}else{
					// for uniquness 
					String unique_id = summary.attr("id").trim();
					String[] id = unique_id.split("_");
					String fakeLink = SITE_HOME_URL + "?page_id=" + id[2];
					resultMap.put(Constants.KEY_PAGE_URL, fakeLink);
				}
				
				Element li5 = liElem.get(4);
				String liText = li5.text().trim();
				if(liText.startsWith("Tel:")){
					liText = liText.replace("Tel:", "").trim();
					if(!liText.isEmpty()){
						liText = liText.replace("view phone", "").trim();
						resultMap.put(Constants.KEY_PHONE, liText);
					}
				}
			}
		}
		
		Element col2Elem = summary.getElementsByClass("col-2").first();
		if(col2Elem != null){
			Elements liElem = col2Elem.getElementsByTag("li");
			if(liElem != null){
				Element li = liElem.get(1);
				String liText = li.text().trim();
				if(liText.startsWith("Camp Type:")){
					liText = liText.replace("Camp Type:", "").trim();
					if(!liText.isEmpty()){
						if(liText.length()>100){
							liText = liText.substring(0, 100);
						}
						resultMap.put(Constants.KEY_ACTIVITY_TYPE, liText);
						if(liText.contains("Overnight Camps")){
							resultMap.put(Constants.KEY_PROGRAM_TYPE, "Overnight Camps");
						}else if(liText.contains("Day Camps")){
							resultMap.put(Constants.KEY_PROGRAM_TYPE, "Day Camps");
						}
					}
				}
			}
		}
		
		Element col3Elem = summary.getElementsByClass("col-3").first();
		if(col3Elem != null){
			Elements liElem = col3Elem.getElementsByTag("li");
			if(liElem != null){
				Element genderElem = liElem.get(2);
				resultMap = fetchGender(genderElem, resultMap);
				
				Element ageElem = liElem.get(3);
				resultMap = fetchAgeRange(ageElem, resultMap);
			}
		}
		
		
		resultMap.put(Constants.KEY_HAS_CAMP, "Y");
		resultMap.put(Constants.KEY_SITE_NAME, SOURCE);
		System.out.println(resultMap.toString());
		if(resultMap.containsKey(Constants.KEY_PAGE_URL)){
			if(!providerDAO.isRecordExist(resultMap.get(Constants.KEY_PAGE_URL).toString()))
				providerDAO.addProvider(resultMap);
		}
	}
	
	
	private void parsePage(String pageUrl, String html) throws Exception{
		
		ProviderDAO providerDAO = new ProviderDAO(dbCon);
		HashMap<String, Object> resultMap = new HashMap<String, Object>(); 
		resultMap.put(Constants.KEY_SITE_NAME, SOURCE);
		resultMap.put(Constants.KEY_PAGE_URL, pageUrl);
		resultMap.put(Constants.KEY_IS_EXTERNAL_URL, "N");
		Document page = Jsoup.parse(html, pageUrl);
		Element mainElement = page.getElementById("printdata");
		if(mainElement != null){
			Element proElem = mainElement.getElementsByTag("h2").first();
			if(proElem != null){
				String providerName = proElem.text().trim();
				resultMap.put(Constants.KEY_PROVIDER_NAME, providerName);
			}
			
			Element logoElem = mainElement.getElementsByClass("no-link").first();
			if(logoElem != null){
				Element logoImg = logoElem.getElementsByTag("img").first();
				if(logoImg != null){
					String imgsrc = logoImg.absUrl("src").trim();
					if(!imgsrc.isEmpty())
						resultMap.put(Constants.KEY_LOGO_URL, imgsrc);
				}
			}
			
			Element descElem = mainElement.getElementById("camp-detail-description");
			if(descElem != null){
				Element pElem = descElem.getElementsByTag("p").first();
				if(pElem != null){
					String desc = pElem.text().trim();
					resultMap.put(Constants.KEY_DESCRIPTION, desc);
				}
			}
			
			Element contElem = mainElement.getElementById("camp-contact");
			if(contElem != null){
				Elements liElem = contElem.getElementsByTag("li");
				if(liElem != null){
					for (Element li : liElem) {
						String liText = li.text().trim();
						if(liText.startsWith("Camp Contact:")){
							liText = liText.replace("Camp Contact:", "").trim();
							if(!liText.isEmpty()){
								resultMap.put(Constants.KEY_CONTACT_NAME, liText);
							}
						}else if(liText.startsWith("Phone:")){
							liText = liText.replace("Phone: view phone", "").trim();
							resultMap.put(Constants.KEY_PHONE, liText);
						}else if(liText.startsWith("Website:")){
							liText = liText.replace("Website:", "").trim();
							resultMap.put(Constants.KEY_WEBSITE, liText);
						}
					}
					Element li1 = liElem.get(1);
					String liText = li1.text().trim();
					if(liText.startsWith("Camp Address:")){
						liText = liText.replace("Camp Address:", "").trim();
						if(!liText.isEmpty()){
							resultMap.put(Constants.KEY_STREET_ADDRESS, liText);
						}
						Element li2 = liElem.get(2);
						liText = li2.text().trim();
						if(!liText.isEmpty()){
							String[] add = liText.split(",");
							if(add.length == 3){
								resultMap.put(Constants.KEY_CITY, add[0].trim());
								resultMap.put(Constants.KEY_STATE, add[1]);
								String[] zip = add[2].split(" ");
								resultMap.put(Constants.KEY_ZIP_CODE, zip[zip.length-1].trim());
							}else if(add.length == 2){
								resultMap.put(Constants.KEY_STATE, add[0].trim());
								String[] zip = add[1].split(" ");
								resultMap.put(Constants.KEY_ZIP_CODE, zip[zip.length-1].trim());
							}
							
						}
					}
				}
			}
			
			Element campDetailElem = mainElement.getElementById("camp-details");
			if(campDetailElem != null){
				Element detailElem = campDetailElem.getElementsByClass("camp-details-col").first();
				if(detailElem != null){
					Elements liElem = detailElem.getElementsByTag("li");
					for (Element li : liElem) {
						String liText = li.text().trim();
						if(liText.startsWith("Camp Type:")){
							String actType = "";
							Elements aElem = li.getElementsByTag("a");
							if(aElem != null){
								for (Element a : aElem) {
									if(actType.length()<80){
										actType += a.text().trim() + "\t";
									}
								}
								if(!actType.isEmpty()){
									resultMap.put(Constants.KEY_ACTIVITY_TYPE, actType);
								}
							}
						}else if(liText.startsWith("Camp Activity:")){
							ArrayList<String> activities = new ArrayList<String>();
							Elements aElem = li.getElementsByTag("a");
							if(aElem != null){
								for (Element a : aElem) {
									String act = a.text().trim();
									activities.add(act);
								}
								if(activities.size()>0){
									resultMap.put(Constants.KEY_ACTIVITIES, activities);
								}
							}
						}else if(liText.startsWith("Age Range:")){
							resultMap = fetchAgeRange(li, resultMap);
						}else if(liText.startsWith("Gender:")){
							resultMap = fetchGender(li, resultMap);
						}
					}
				}
			}
		}
		resultMap.put(Constants.KEY_HAS_CAMP, "Y");
		System.out.println(resultMap.toString());
		LOG.info("Result : " + resultMap.toString());
		providerDAO.addProvider(resultMap);
		
	}
	
	
	private String fetchWebsite(Element liElem){
		String liText = liElem.text().trim();
		if(liText.startsWith("Website:")){
			liText = liText.replace("Website:", "").trim();
			if(!liText.isEmpty()){
				return liText;
			}
		}
		return null;
	}
	
	private HashMap<String, Object> fetchGender(Element genderElem, HashMap<String, Object> resultMap){
		String liText = genderElem.text().trim();
		if(liText.startsWith("Gender:")){
			liText = liText.replace("Gender:", "").trim();
			if(!liText.isEmpty()){
				if(liText.equals("Coed")){
					resultMap.put(Constants.KEY_PROGRAM_FOR, Constants.PROGRAM_FOR_COED);
				}else if(liText.equals("Female")){
					resultMap.put(Constants.KEY_PROGRAM_FOR, Constants.PROGRAM_FOR_GIRLS);
				}else if(liText.equals("Male")){
					resultMap.put(Constants.KEY_PROGRAM_FOR, Constants.PROGRAM_FOR_BOYS);
				}
			}
		}
		return resultMap;
	}
	
	private HashMap<String, Object> fetchAgeRange(Element ageElem, HashMap<String, Object> resultMap){
		String liText = ageElem.text().trim();
		if(liText.startsWith("Age Range:")){
			liText = liText.replace("Age Range:", "").trim();
			if((!liText.isEmpty() && !liText.equals("Any"))){
				String[] age = liText.split(" - ");
				String fromAge = age[0];
				String toAge = age[1];
				if(fromAge.equals("Any")){
					fromAge = String.valueOf(0);
				}
				if(toAge.equals("Any")){
					toAge = String.valueOf(99);
				}
				resultMap.put(Constants.KEY_FROM_AGE, fromAge);
				resultMap.put(Constants.KEY_TO_AGE, toAge);
			}
		}
		return resultMap;
	}

}
