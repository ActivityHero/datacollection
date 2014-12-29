package com.ah.scraper.scrapers;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;   
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.FormElement;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import com.ah.scraper.common.Constants;
import com.ah.scraper.common.DBConnection;
import com.ah.scraper.common.ScraperUtil;
import com.ah.scraper.dao.MainPageDAO;
import com.ah.scraper.dao.ProviderDAO;  


public class ACACampScraper implements IScraper {
	private static Logger LOG = LoggerFactory.getLogger(ACACampScraper.class);
	public static int currentRecord = 0;
    public static String SITE_HOME_URL = "http://find.acacamps.org/index.php";
    public static String SOURCE = "acacamps";
    public static String searchOption = "day_and_overnight";
    
	private DBConnection dbCon;
	 
	private int MaxThreads = 127;
	private int ThreadsAtATime = 10; 
	private static int activeThreads = 0;  
	private static int createdThreads = 0;  
	public static boolean hasMoreData=true; 
	public synchronized void setMoreData(boolean b){
		hasMoreData = b;
	}
	public synchronized boolean getMoreData(){
		return hasMoreData;
	}
		
	static String createPageUrl(String id){
		return  "http://find.acacamps.org/camp_profile.php?back=search&camp_id="+ id;
	}	
	public ACACampScraper(){ 
	}
	
	public void run(){
		try { 
			LOG.info("Scraper run called");
			this.startScraping();
		} catch (Exception e) { 
			LOG.error("err",e);
		}
	}
    public void startScraping() throws Exception { 
		LOG.info("ACACampScraper launched");  
		while (getCreatedThreads() < MaxThreads) {  	 
			if(getActiveThreads() < ThreadsAtATime){
				currentRecord = currentRecord + 25;
				LOG.info("-------------------Going to create thread---------------------------"+getCreatedThreads() );
				ScraperThread t = new ScraperThread(currentRecord); 
				t.start();  
				setActiveThreads(1);
				setCreatedThreads(1);
			}  
		} 
    }
    
    private class ScraperThread extends Thread { 
    	private int fromRecord; 
    	DBConnection dbConT = new DBConnection(); 
		public ScraperThread(int fromRecord) { 
			this.fromRecord = fromRecord; 
		}	
		public void run(){
			LOG.info("-------------------thread run called---------------------------");
			try{
				
				Document page = Jsoup.connect(SITE_HOME_URL)
				.timeout(Constants.REQUEST_TIMEOUT).userAgent(Constants.USER_AGENT).get();		

				FormElement searchForm = ((FormElement) page.getElementById("search_form"));
				if(searchForm == null){
					LOG.info(SOURCE+ ": search form not found. Existing...");
					return;
				}
				Connection con = searchForm.submit();
				con.timeout(Constants.REQUEST_TIMEOUT).data("facets[camp_type]", searchOption).data("result_start", "" + fromRecord);
		
		 		Document resultPage2 = con.post();
		 
				LOG.info(SOURCE+ ": search result page submitted from record="+fromRecord);
				Element resultTable = resultPage2.getElementsByClass("search_result_listing").first();
				if(resultTable == null){
					LOG.info(SOURCE+": no search results found, existing...");
					return;
 				}
				Elements rows = resultTable.select("tr");

				int resultCount = 0;

				for (Element row : rows) {
					Elements tds = row.select("td");
					if (tds.size() > 1) {
						Element column = tds.first();
						Element h3 = column.getElementsByTag("h3").first();
						Element link = h3.getElementsByTag("a").first(); 
						parsePage(link.absUrl("href"), link.baseUri());  
						resultCount++; 
					}
				} 
				
				if(resultCount == 0){
					 setMoreData(false);
				}
				LOG.info(SOURCE+": Processed rows: " + resultCount);				
			}
			catch(Exception e){
				LOG.error("err",e);
			}
			finally{
				//close connections
				try{
					dbConT.close();
				}
				catch(Exception e){
					LOG.error("err",e);
				}		
				setActiveThreads(-1);
			}
			


		}
		public void parsePage(String pageUrl, String baseUri) throws Exception {
			MainPageDAO htmlDAO = new MainPageDAO(dbConT);
			ProviderDAO providerDAO  = new ProviderDAO(dbConT);
			LOG.info(SOURCE+": GET Camp: " + pageUrl);
			Document campDetails = null;
			String htmlPage = htmlDAO.getHtmlPage(pageUrl, false);
			if(htmlPage != null){
				campDetails = Jsoup.parse(htmlPage,baseUri);
			}else{
				ScraperUtil.sleep();
				campDetails = Jsoup.connect(pageUrl).timeout(Constants.REQUEST_TIMEOUT).userAgent(Constants.USER_AGENT).get();
				htmlDAO.addHtmlPage(pageUrl, campDetails.toString(), SOURCE);
			}
			
			HashMap<String, Object> map = new HashMap<String, Object>();
			map.put(Constants.KEY_HAS_CAMP, "Y");

			// get id
			URL url = new URL(pageUrl);
			Map<String, List<String>> params = ScraperUtil.splitQuery(url);
			if (params != null) {
				List<String> ids = params.get("camp_id");
				if (ids != null && ids.size() > 0) {
					map.put(Constants.KEY_ID, ids.get(0));
				}
			}
			
			Element content = campDetails.getElementById("content");
			Elements h1s = content.getElementsByTag("h1");
			if (h1s == null || h1s.size() == 0) {
				LOG.info(SOURCE+": Provider not found on the main page - " + pageUrl);
				return;
			}

			String providerName = h1s.first().text().trim();
			map.put(Constants.KEY_PROVIDER_NAME, providerName);
			Elements logoHolders = campDetails.getElementById("camp_profile_logo_holder").getElementsByTag("img");

			if (logoHolders.size() > 1) {
				Element logo = logoHolders.get(1);
				String logoUrl = logo.absUrl("src");
				map.put(Constants.KEY_LOGO_URL, logoUrl);
			}

			Element slideShow = campDetails.getElementById("slideshow");
			if (slideShow != null) {
				Elements images = slideShow.getElementsByTag("img");
				if (images != null && images.size() > 0) {
					ArrayList<String> photoUrls = new ArrayList<String>();
					for (Element img : images) {
						String photoLink = img.absUrl("src");
						if (photoLink != null) {
							photoUrls.add(photoLink);
						}
					}
					
					if (photoUrls.size() > 0) {
						map.put(Constants.KEY_PHOTO_URL, photoUrls);
					}
				}

			}

			Element addressBox = campDetails.getElementsByClass("address_box").first();
			getAddress(addressBox, map);

			Element descriptionElement = campDetails.getElementById("description_long_1");
			if(descriptionElement == null){
				descriptionElement = campDetails.getElementById("description_short_1");
			}
			map.put(Constants.KEY_DESCRIPTION, descriptionElement.text().trim());

			Element programTable = campDetails.getElementsByClass("grid").first();
			Elements rows = programTable.select("tr");

			ArrayList<HashMap<String, Object>> programs = new ArrayList<HashMap<String, Object>>();

			int i = 0;
			for (Element row : rows) {
				if (i > 0) {
					// System.out.println("Parent page = "+pageUrl);
					HashMap<String, Object> program = getProgram(row,pageUrl);
					if (program != null) {
						programs.add(program);
					}
				}
				i++;
			}
			
			map.put(Constants.KEY_ALL_PROGRAMS, programs);

			JSONObject json = new JSONObject(map);

			LOG.info("result : "+ json.toString());
			//Write results in provider table
			providerDAO.addProvider(getResultMap(map));
		}	
		
		 HashMap<String, Object> getResultMap(Map<String, Object> rs) throws JSONException{
				HashMap<String, Object> map = new HashMap<String, Object>();
				JSONObject json = new JSONObject(rs);
				
				map.put(Constants.KEY_SITE_NAME, SOURCE);
				String page_url = createPageUrl(json.getString(Constants.KEY_ID));
				map.put(Constants.KEY_PAGE_URL, page_url);

				if(json.has(Constants.KEY_PROVIDER_NAME))
					map.put(Constants.KEY_PROVIDER_NAME, json.getString(Constants.KEY_PROVIDER_NAME));

				if(json.has(Constants.KEY_LOGO_URL))
					map.put(Constants.KEY_LOGO_URL, json.getString(Constants.KEY_LOGO_URL));

				if(rs.containsKey(Constants.KEY_PHOTO_URL))
					map.put(Constants.KEY_PHOTO_URL, rs.get(Constants.KEY_PHOTO_URL));

				if(json.has(Constants.KEY_WEBSITE))
					map.put(Constants.KEY_WEBSITE, json.getString(Constants.KEY_WEBSITE));

				if(json.has(Constants.KEY_DESCRIPTION))
					map.put(Constants.KEY_DESCRIPTION, json.getString(Constants.KEY_DESCRIPTION));

				if(json.has(Constants.KEY_CONTACT_NAME))
					map.put(Constants.KEY_CONTACT_NAME, json.getString(Constants.KEY_CONTACT_NAME));

				if(json.has(Constants.KEY_STREET_ADDRESS))
					map.put(Constants.KEY_STREET_ADDRESS, json.getString(Constants.KEY_STREET_ADDRESS));

				if(json.has(Constants.KEY_CITY))
					map.put(Constants.KEY_CITY, json.getString(Constants.KEY_CITY));

				if(json.has(Constants.KEY_STATE))
					map.put(Constants.KEY_STATE, json.getString(Constants.KEY_STATE));

				if(json.has(Constants.KEY_ZIP_CODE))
					map.put(Constants.KEY_ZIP_CODE, json.getString(Constants.KEY_ZIP_CODE));

				if(json.has(Constants.KEY_PHONE))
					map.put(Constants.KEY_PHONE, json.getString(Constants.KEY_PHONE));

				if(json.has(Constants.KEY_EMAIL))
					map.put(Constants.KEY_EMAIL, json.getString(Constants.KEY_EMAIL));

				if(json.has(Constants.KEY_LOCATION))
				map.put(Constants.KEY_LOCATION, json.getString(Constants.KEY_LOCATION));

				if(json.has(Constants.KEY_HAS_CAMP))
					map.put(Constants.KEY_HAS_CAMP, json.getString(Constants.KEY_HAS_CAMP));

				String price_type = "";

				Integer from_age = null;
				Integer to_age = null;
				Float to_price = null;
				Float from_price = null;
				String to_grade = null;
				String from_grade = null;
				ArrayList<String> activities = new ArrayList<String>();
				
				@SuppressWarnings("unchecked")
				ArrayList<HashMap<String, Object>> programs = (ArrayList<HashMap<String, Object>>) rs.get(Constants.KEY_ALL_PROGRAMS);
				for(HashMap<String, Object> program :programs){
					JSONObject pRS = new JSONObject(program);
					if (pRS.has(Constants.KEY_FROM_AGE)  && (from_age == null || from_age > pRS.getInt(Constants.KEY_FROM_AGE))) {
						from_age = pRS.getInt(Constants.KEY_FROM_AGE);
					}

					if (pRS.has(Constants.KEY_TO_AGE) && (to_age == null || to_age < pRS.getInt(Constants.KEY_TO_AGE))) {
						to_age = pRS.getInt(Constants.KEY_TO_AGE);
					}

					if (pRS.has(Constants.KEY_FROM_PRICE) && (from_price == null || from_price > pRS.getDouble(Constants.KEY_FROM_PRICE))) {
						from_price = (float) pRS.getDouble(Constants.KEY_FROM_PRICE);
					}
					
					if (pRS.has(Constants.KEY_TO_PRICE) && (to_price == null || to_price < pRS.getDouble(Constants.KEY_TO_PRICE))) {
						to_price = (float) pRS.getDouble(Constants.KEY_TO_PRICE);
					}

					if ((to_grade == null || to_grade.isEmpty())) {
						if(pRS.has(Constants.KEY_TO_GRADE))
							to_grade = pRS.getString(Constants.KEY_TO_GRADE);
					} else if(pRS.has(Constants.KEY_TO_GRADE)){
						String s = pRS.getString(Constants.KEY_TO_GRADE);
						if (s != null && getGradeFromStr(to_grade, 0) < getGradeFromStr(s, -1)) {
							to_grade = s;
						}
					}

					if (from_grade == null || from_grade.isEmpty()) {
						if(pRS.has(Constants.KEY_FROM_GRADE))
							from_grade = pRS.getString(Constants.KEY_FROM_GRADE);
					} else if(pRS.has(Constants.KEY_FROM_GRADE)) {
						String s = pRS.getString(Constants.KEY_FROM_GRADE);
						if (s != null && getGradeFromStr(from_grade, 0) > getGradeFromStr(s, 1000)) {
							from_grade = s;
						}
					}

					if(pRS.has(Constants.KEY_PRICE_TYPE))
						price_type = pRS.getString(Constants.KEY_PRICE_TYPE);

					String actStr = ScraperUtil.tabbedStrFromMap(program, Constants.KEY_ACTIVITIES);
					if (actStr != null && !actStr.isEmpty()) {
						activities.add(actStr);
					}
				}
				
				if((from_age == null || from_age == -1) && from_grade != null && !from_grade.isEmpty()){
					from_age = getGradeFromStr(from_grade, -1);
				}
				if((to_age == null || to_age == -1) && to_grade != null && !to_grade.isEmpty()){
					to_age = getGradeFromStr(to_grade, -1);
				}

				map.put(Constants.KEY_PRICE_TYPE, price_type);
				map.put(Constants.KEY_FROM_AGE, String.valueOf(from_age));
				map.put(Constants.KEY_TO_AGE, String.valueOf(to_age));
				map.put(Constants.KEY_TO_PRICE, String.valueOf(to_price));
				map.put(Constants.KEY_FROM_PRICE, String.valueOf(from_price));
				map.put(Constants.KEY_TO_GRADE, to_grade);
				map.put(Constants.KEY_FROM_GRADE, from_grade);
				map.put(Constants.KEY_ACTIVITIES, activities);

				LOG.info(new JSONObject(map).toString());

				return map;
				
			}
		 
		 void getAddress(Element addressBox, HashMap<String, Object> map) {

				for (Element anchor : addressBox.getElementsByTag("a")) {
					String link = anchor.absUrl("href");
					if (link.startsWith("mailto")) {
						String email = anchor.text().trim();
						map.put(Constants.KEY_EMAIL, email);
					} else if (link.startsWith("http")) {
						String website = anchor.text().trim();
						map.put(Constants.KEY_WEBSITE, website);
					}
				}

				Element hr = addressBox.getElementsByTag("hr").first();

				int i = 0;
				Node currentNode = hr.previousSibling();
				String cityStateZipText = null;
				String streetText = null;
				while (currentNode != null) {
					if (currentNode.getClass().equals(Element.class)) {
						Element element = (Element) currentNode;
						if (element.text().trim().contains("Location")) {
							break;
						}
					}
					if (i == 20)
						break;
					i++;

					String text = currentNode.outerHtml().replace("&nbsp;", " ").trim();
					if (text.contains("view map") || text.equals("")
							|| text.equals("<br>")) {

					} else {
						if (cityStateZipText == null) {
							cityStateZipText = text;
							String[] tokens = cityStateZipText.split(",");

							if (tokens.length > 1) {
								String city = tokens[0];
								map.put(Constants.KEY_CITY, city);
								String stateZip = tokens[1].trim();
								String[] arr = stateZip.split(" ");
								if (arr.length > 0) {
									String state = arr[0];
									map.put(Constants.KEY_STATE, state);
								} else {
									LOG.info("Addrees Format exception: "
											+ addressBox.baseUri());
								}
								if (arr.length > 1) {
									String zip = arr[1];
									map.put(Constants.KEY_ZIP_CODE, zip);
								}

							} else {
								LOG.info("Addrees Format exception: "
										+ addressBox.baseUri());
							}

						} else if (streetText == null) {
							streetText = text;
							map.put(Constants.KEY_STREET_ADDRESS, streetText);
						} else {
							break;
						}
					}
					currentNode = currentNode.previousSibling();
				}

				for (Node node : addressBox.childNodes()) {
					if (node.getClass().equals(Element.class)) {
						Element element = (Element) node;
						if (element.text().equals("Location")) {

						} else if (element.text().equals("Contact")) {
							Node contactNameNode = element.nextSibling();
							if (!map.containsKey(Constants.KEY_CONTACT_NAME)) {
								// map.put(KEY_CONTACT_NAME,
								// contactNameNode.outerHtml().replace("&nbsp;", " "));
							}
							Node phoneNode = contactNameNode.nextSibling()
									.nextSibling();
							String phoneText = phoneNode.outerHtml()
									.replace("&nbsp;", " ").trim();
							// summer (269) 521-3855 Winter (269) 352-3379
							// Pattern pattern =
							// Pattern.compile("\\(?(\\d{3})\\)?-(\\d{3})-(\\d{4})");
							Pattern pattern = Pattern.compile("([0-9-./( )]+)");
							Matcher matcher = pattern.matcher(phoneText);
							if (matcher.find()) {
								String phone = matcher.group();
								map.put(Constants.KEY_PHONE, phone);
							}
						}

					} else {
						String text = node.outerHtml().trim();
						if (text.startsWith("Camp Director:")) {
							String[] textArr = text.split(":");
							if (textArr.length > 1) {
								String campDirector = textArr[1];
								map.put(Constants.KEY_CONTACT_NAME, campDirector);
							}

						} else if (ScraperUtil.validEmail(text)) {
							if (!map.containsKey(Constants.KEY_EMAIL))
								map.put(Constants.KEY_EMAIL, text);
						}

					}

				}
			}
		 
		 HashMap<String, Object> getProgram(Element row, String baseUri) throws Exception {
				MainPageDAO htmlDAO = new MainPageDAO(dbCon);
				Elements tds = row.select("td");
				if (tds.size() > 5) {
					HashMap<String, Object> map = new HashMap<String, Object>();
					
					map.put(Constants.KEY_PRICE_TYPE, "per_session");

					Element nameElement = tds.get(0);
					String name = nameElement.text().trim();
					map.put(Constants.KEY_PROGRAM_NAME, name);
					Element linkEle = nameElement.getElementsByTag("a").first();
					String link = linkEle.absUrl("href");

					Element forElement = tds.get(1);
					String forText = forElement.text().trim();
					map.put(Constants.KEY_PROGRAM_FOR, forText);

					Element typeElement = tds.get(2);
					String type = typeElement.text().trim();
					map.put(Constants.KEY_PROGRAM_TYPE, type);

					Element ageGradeElement = tds.get(3);
					for (Node node : ageGradeElement.childNodes()) {
						if (node.getClass().equals(Element.class)) {
							// br
						} else {

							String[] text = node.outerHtml().trim().split(" - ");
							String fromValue = null;
							String toValue = null;
							if (text.length > 0) {
								fromValue = text[0];
								if (text.length > 1) {
									toValue = text[1];
								} else {
									toValue = fromValue;
								}

								boolean isAge = toValue.contains("year")
										|| toValue.contains("yr");

								if (isAge) {
									// age
									fromValue = fromValue.replace("years", "")
											.replace("yrs", "").replace(" ", "").trim();
									toValue = toValue.replace("years", "")
											.replace("yrs", "").replace(" ", "").trim();
									map.put(Constants.KEY_FROM_AGE, fromValue);
									map.put(Constants.KEY_TO_AGE, toValue);

								} else {
									// grade
									map.put(Constants.KEY_FROM_GRADE, fromValue);
									map.put(Constants.KEY_TO_GRADE, toValue);
								}

							} else {
								LOG.info("Age grade format exception: "
										+ row.baseUri());
							}
						}
					}

					Element trsportationElement = tds.get(4);
					String transportation = trsportationElement.text().trim();
					map.put(Constants.KEY_PROGRAM_TRANSPORTATION, transportation);

					Element priceElement = tds.get(5);
					String[] prices = priceElement.text().trim().split(" - ");
					String fromValue = null;
					String toValue = null;
					if (prices.length > 0) {
						fromValue = prices[0].replace("$", "").replace(",", "").trim();
						if (prices.length > 1) {
							toValue = prices[1].replace("$", "").replace(",", "")
									.trim();
						} else {
							toValue = fromValue;
						}
						map.put(Constants.KEY_FROM_PRICE, fromValue);
						map.put(Constants.KEY_TO_PRICE, toValue);

					} else {
						LOG.info("Price format exception: " + row.baseUri());
					}

					// get id
					URL url = new URL(link);
					Map<String, List<String>> params = ScraperUtil.splitQuery(url);
					if (params != null) {
						List<String> ids = params.get("program_id");
						if (ids != null && ids.size() > 0) {
							map.put(Constants.KEY_PROGRAM_ID, ids.get(0));
						}
					}

					ArrayList<String> activities = new ArrayList<String>();
					//LOG.info("GET Program: " + link);
					Document programDetails = null;
					String pageData = htmlDAO.getHtmlPage(link, true);
					if(pageData == null){
						ScraperUtil.sleep();
						programDetails = Jsoup.connect(link)
								.timeout(Constants.REQUEST_TIMEOUT).get();
						
						htmlDAO.addChildHtmlPage(link, baseUri, programDetails.toString(), SOURCE);
					}else{
						programDetails = Jsoup.parse(pageData,baseUri);
					}
					
					
					Element content = programDetails.getElementById("content");
					Element element1 = content.getElementsByClass("two_column_big")
							.first();
					Element element2 = element1.getElementsByClass("three_column")
							.first();
					if (element2 != null) {
						Elements tables = element2.getElementsByTag("table");
						Element table = tables.last();
						if (!table.className().equals("grid")) {
							Elements tdss = table.getElementsByTag("tr").first()
									.getElementsByTag("td");
							for (Element td : tdss) {
								Element ul = td.getElementsByTag("ul").first();
								for (Element li : ul.children()) {
									activities.add(li.text().trim());
								}
							}
						}
					}

					map.put(Constants.KEY_ACTIVITIES, activities);

					return map;
				}

				return null;
			}
    } //end thread
    

	
	 
	
	static Integer getGradeFromStr(String str, Integer defaultValue) {

		if (str == null) {
			return defaultValue;
		}
		Integer grade = defaultValue;
		
		String lc = str.toLowerCase(); 
		if (lc.contains("preschool"))
			grade = 3; //3 yrs
		else if (lc.contains("pre-kindergarten"))
			grade = 4; //4 yrs
		else if (lc.contains("kindergarten"))
			grade = 5; //5 yrs
		else {
			str = str.replaceAll("(st|rd|th|nd)", "").trim();
			if (!str.isEmpty()) {
				grade = Integer.valueOf(str)+5;
			}
		}
		
		return grade;
		
	}
	
	/**
	 * @return the activeThreads
	 */
	public static synchronized int getActiveThreads() {
		return activeThreads;
	}
	/**
	 * @param activeThreads the activeThreads to set
	 */
	public static  synchronized void setActiveThreads(int cnt) {
		activeThreads = activeThreads + cnt;
	}
	/**
	 * @return the createdThreads
	 */
	public static synchronized int getCreatedThreads() {
		return createdThreads;
	}
	/**
	 * @param createdThreads the createdThreads to set
	 */
	public static synchronized void setCreatedThreads(int cnt) {
		createdThreads = createdThreads+cnt;
	}


	

}
