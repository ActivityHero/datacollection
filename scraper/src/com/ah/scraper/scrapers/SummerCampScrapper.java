package com.ah.scraper.scrapers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class SummerCampScrapper implements IScraper {
	private static Logger LOG = LoggerFactory.getLogger(SummerCampScrapper.class);
	public static int currentRecord = 1;
    public static String SITE_HOME_URL = "http://www.summercamps.com/";
    public static String SOURCE = "summercamps";
	private DBConnection dbCon;
	
	public SummerCampScrapper(){
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
		LOG.info("SummerCampScrapper launched");
		//MainPageDAO htmlDAO = new MainPageDAO(dbCon);
		boolean hasMoreData = true;

		while (hasMoreData) {
			hasMoreData = false;
			SummerCampScrapper.currentRecord += 25;
			
			String pageUrl =  SITE_HOME_URL;
			LOG.info(SOURCE+": Getting data from = "+pageUrl);
			ScraperUtil.sleep();
			
			Document page = Jsoup.connect(pageUrl).timeout(Constants.REQUEST_TIMEOUT).get();
			
			Element categoryContainer = page.getElementsByClass("catslist").first();
			Element outerContainer = categoryContainer.getElementsByTag("p").first();
			Elements locationLinkTags = outerContainer.getElementsByTag("a");
			for(Element locationLinkTag : locationLinkTags){
				Element campListContainer = getResultContainer(locationLinkTag.attr("href"));
				parseCellContainer(campListContainer);				
				Element pagingContainer = campListContainer.getElementsByClass("paging").first();
				if(pagingContainer != null){
					Element pagingValues = pagingContainer.getElementsByTag("select").first();
					Elements pageNums  = pagingValues.getElementsByTag("option");
					for(int i=2;i <= pageNums.size();i++){
						String nextPageUrl = locationLinkTag.attr("href")+"more"+i+".html";
						Element nextPageListContainer = getResultContainer(nextPageUrl);
						parseCellContainer(nextPageListContainer);
					}
				}				
			}
		}
		LOG.info("SummerCampScrapper finished");
	}
    
    public Element getResultContainer(String url) throws IOException{
    	LOG.info(SOURCE+": Camp List page URL = "+url);
    	Document campsListPage = Jsoup.connect(url).timeout(Constants.REQUEST_TIMEOUT).get();
		Element campListContainer = campsListPage.getElementById("hiddenph");
		return campListContainer;
    }
    
    public void parseCellContainer(Element campListContainer) throws Exception{
    	Elements rows = campListContainer.getElementsByClass("linklisting");
    	for(Element row : rows){
    		Element linkTag = row.getElementsByTag("h4").first().getElementsByTag("a").first();
    		if(linkTag != null){
    			String detailPageUrl = linkTag.attr("href");
    			parseDetailPage(detailPageUrl);
    		}    		
    	}
    }
    
    public void parseDetailPage(String pageUrl) throws Exception{
    	MainPageDAO htmlDAO = new MainPageDAO(dbCon);
		ProviderDAO providerDAO  = new ProviderDAO(dbCon);
		Document campDetails = null;
		
		String pageHtml = htmlDAO.getHtmlPage(pageUrl, false);
		if(pageHtml == null){
			LOG.info("GET Camp Detail page url: " + pageUrl);
			ScraperUtil.sleep();
			campDetails = Jsoup.connect(pageUrl).timeout(Constants.REQUEST_TIMEOUT).get();
			htmlDAO.addHtmlPage(pageUrl, campDetails.toString(),SOURCE);
		}else{
			campDetails = Jsoup.parse(pageHtml,pageUrl);
		}
		//remove all iframes
		campDetails.select("iframe").remove();
		
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put(Constants.KEY_SITE_NAME, SOURCE);
		map.put(Constants.KEY_IS_EXTERNAL_URL, Constants.EXTERNAL_URL_NO);
		map.put(Constants.KEY_PAGE_URL, pageUrl);
		
		Element campDetailElem = campDetails.getElementsByClass("campdetails").first();
		
		//Get provider name
		Element providerNameElem = campDetailElem.getElementsByTag("h2").first();
		if(providerNameElem != null)
			map.put(Constants.KEY_PROVIDER_NAME, providerNameElem.text().trim());
		
		//Get camp description
		Element descriptionElem = campDetailElem.getElementsByClass("description").first();
		if(descriptionElem != null){
			String description = descriptionElem.html().trim();
			map.put(Constants.KEY_DESCRIPTION, description);
			
			Element logoUrlElem = descriptionElem.getElementsByTag("img").first();
			if(logoUrlElem != null && logoUrlElem.hasAttr("src")){
				String logoUrl = logoUrlElem.attr("src");
				map.put(Constants.KEY_LOGO_URL, logoUrl);
			}
		}
		
		//Get camp activities
		Element activitiesElem = campDetailElem.getElementsByClass("act").first();
		if(activitiesElem != null){
			List<String> activitiesList = new ArrayList<String>(Arrays.asList(activitiesElem.text().replaceAll("Camp Activities:", "").trim().split("\\\u2022")));
			map.put(Constants.KEY_ACTIVITIES, activitiesList);
		}
		//Get camp image
		Element campImgsElem = campDetails.getElementById("multimedia");
		if(campImgsElem != null){
			Element campImgGalleryElem = campImgsElem.getElementsByClass("slideshow").first();
			if(campImgGalleryElem != null){
				Elements imgElem = campImgGalleryElem.getElementsByTag("img");
				ArrayList<String> photoUrls = new ArrayList<String>();
				for(Element img : imgElem){
					String photoUrl = img.absUrl("src").trim();
					if(photoUrl != null)
						photoUrls.add(photoUrl);
				}
				if(photoUrls.size() > 0)
					map.put(Constants.KEY_PHOTO_URL, photoUrls);
			}
		}
		
		//Get camp details
		Element detailSectionElem = campDetails.getElementsByClass("details_right").first();
		
		//Get all paragraph tags
		Elements detailsElem = detailSectionElem.getElementsByTag("p");
		if(detailsElem != null){
			for(Element detailElem : detailsElem){
				String info = detailElem.html();
				//Case for address
				if(info.indexOf("Camp Address:") != -1){
					//Set street address
					String[] tokens = info.split("<br>");
					
					if(tokens.length > 0)
						map.put(Constants.KEY_STREET_ADDRESS, tokens[1].trim().replaceAll("\\.",""));
					
					if(tokens.length > 1 && tokens[2].indexOf(",") != -1){
						tokens[2] = tokens[2].replaceFirst(",", "&");
						String[] cityAndStateTokens = tokens[2].split("&");
						if(cityAndStateTokens.length >= 0)
							map.put(Constants.KEY_CITY, cityAndStateTokens[0].trim().replaceAll("\\.",""));
						
						if(cityAndStateTokens.length > 0 && cityAndStateTokens[1].indexOf(" ") != -1){
							String[] stateAndZipTokens = cityAndStateTokens[1].trim().split(" ");
							if(stateAndZipTokens.length >= 0)
								map.put(Constants.KEY_STATE, stateAndZipTokens[0].trim().replaceAll("\\.",""));
							
							if(stateAndZipTokens.length > 0)
								map.put(Constants.KEY_ZIP_CODE, stateAndZipTokens[stateAndZipTokens.length-1].trim().replaceAll("\\.",""));
						}
					}
				}
				
				//Case for camp type
				if(info.indexOf("Camp Type:") != -1){
					String programTypeStr = detailElem.text();
					if(programTypeStr.indexOf("Cost") != -1)
						programTypeStr = programTypeStr.substring(0, programTypeStr.indexOf("Cost"));
					programTypeStr = programTypeStr.replaceAll("Camp Type:", "").trim();
					map.put(Constants.KEY_PROGRAM_TYPE, programTypeStr);
				}
				
				//Case for price
				if(info.indexOf("Cost/Week:") != -1){
					String pricesStr = detailElem.text();
					pricesStr = pricesStr.substring(pricesStr.indexOf("Cost/Week:")).replaceAll("Cost/Week:", "").replaceAll("\\$", "").replaceAll("/wk", "").trim();
					String[] pricesArr = null;
					if(pricesStr.indexOf("-") != -1){
						pricesArr = pricesStr.split("-");
					}else if(pricesStr.indexOf("to") != -1){
						pricesArr = pricesStr.split("to");
					}
					if(pricesArr != null && pricesArr.length > 0){
						map.put(Constants.KEY_PRICE_TYPE, "per week");
						map.put(Constants.KEY_FROM_PRICE, pricesArr[0].trim());
						if(pricesArr.length > 0)
							map.put(Constants.KEY_TO_PRICE, pricesArr[1].trim());
					}
				}
				
				//Case for gender
				if(info.indexOf("Gender:") != -1){
					Map<String,String> programForMap = new HashMap<String, String>();
					programForMap.put("coed", Constants.PROGRAM_FOR_COED);
					programForMap.put("girls only", Constants.PROGRAM_FOR_GIRLS);
					programForMap.put("boys only", Constants.PROGRAM_FOR_BOYS);
					
					String programFor = detailElem.text().replaceAll("Gender:", "").trim().toLowerCase();
					if(programForMap.containsKey(programFor))
						map.put(Constants.KEY_PROGRAM_FOR, programForMap.get(programFor));
				}
				
				//Case for age
				if(info.indexOf("Ages:") != -1){
					Map<String,String> ageMap = new HashMap<String, String>();
					ageMap.put("all", null);
					Pattern p = Pattern.compile("From (.*) To (.*)", Pattern.CASE_INSENSITIVE);
					Matcher m = p.matcher(detailElem.text().trim());
					while (m.find()) {
						
						if(!ageMap.containsKey(m.group(1).toLowerCase()))
							map.put(Constants.KEY_FROM_AGE, m.group(1));
						
						if(m.groupCount() > 1 && !ageMap.containsKey(m.group(2).toLowerCase()))
							map.put(Constants.KEY_TO_AGE, m.group(2));
					}
				}
				
				//Case for external website url
				Element externalSiteLinkElem = detailElem.getElementsByClass("visit").first();
				if(externalSiteLinkElem != null){					
					String externalUrl = externalSiteLinkElem.attr("href");
					try{
						Document externalSitePage = Jsoup.connect(externalUrl).timeout(Constants.REQUEST_TIMEOUT).get();
						map.put(Constants.KEY_WEBSITE, externalSitePage.baseUri());
					}catch(Exception e){
						LOG.info(e.getMessage());
					}
				}
			}
		}
		
		//Get camp phone no.
		Element phoneElem = detailSectionElem.getElementsByClass("hph").first();
		if(phoneElem != null){
			Element phoneToken = phoneElem.getElementsByTag("p").first();
			String[] phoneNums = phoneToken.html().split("<br>");
			if(phoneNums.length > 0){
				ArrayList<String> phoneNumbers = new ArrayList<String>();
				for (String phone : phoneNums) {
					if(phone != null && !phone.trim().isEmpty())
						phoneNumbers.add(phone);
				}
				if(phoneNumbers.size() > 0){
					map.put(Constants.KEY_PHONE, phoneNumbers);
					map.put(Constants.KEY_PHONE, ScraperUtil.tabbedStrFromMap(map, Constants.KEY_PHONE));
				}
			}
		}
		LOG.info("{}",new JSONObject(map));
		providerDAO.addProvider(map);
    }
}