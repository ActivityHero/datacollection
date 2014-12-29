package com.ah.scraper.scrapers;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
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
import com.ah.scraper.dao.ProviderDAO;
import com.ah.scraper.dao.ZipProcessedDAO;

public class UsaSwimmingScraper implements IScraper {
	
	private static Logger LOG = LoggerFactory.getLogger(UsaSwimmingScraper.class);
	private String SITE_HOME_URL = "http://www.usaswimming.org/USASModules/Mapping/MapService.asmx/GetCityStateZipData";
	public static String SOURCE = "usaswimming";
	private DBConnection dbCon;

	private int minThreads = 1;
	private int maxThreads = 10;
	private int queueSize = 100;
	ThreadPoolExecutor executorService;

	public UsaSwimmingScraper() {
		dbCon = new DBConnection();
		executorService = new ThreadPoolExecutor(minThreads, // core thread pool
																// size
				maxThreads, // maximum thread pool size
				1, // time to wait before resizing pool
				TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(queueSize,
						true), new ThreadPoolExecutor.CallerRunsPolicy());
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
		LOG.info("UsaSwimmingScraper launched");
		ArrayList<String> zips = zipDAO.getUnprocessedZip(SOURCE, queueSize);

		while (zips.size() > 0) {
			for (String zip : zips) {
				executorService.submit(new ScraperThread(zip));
			}
			do {
				// just wait
			} while (executorService.getActiveCount() > minThreads * 3);
			ScraperUtil
					.log("---------------------------------------------------------Going to get more zips now--------------------------------");
			zips = zipDAO.getUnprocessedZip(SOURCE, queueSize - minThreads * 3);
		}
		LOG.info("UsaSwimmingScraper finished");

	}

	private class ScraperThread implements Runnable {
		String zip;
		DBConnection dbConT = new DBConnection();

		public ScraperThread(String zip) {
			this.zip = zip;
		}

		public void run() {

			try {
				// create DAOs
				ZipProcessedDAO zipDAO = new ZipProcessedDAO(dbConT);
				// ProviderDAO providerDAO = new ProviderDAO(dbConT);
				int pageNumber = 1;
				boolean hasMoreData = true;
				while (hasMoreData) {
					hasMoreData = false;
					JSONObject json = prepareQueryParams(zip, pageNumber);
					DefaultHttpClient httpClient = new DefaultHttpClient();
					HttpPost postRequest = new HttpPost(SITE_HOME_URL);

					StringEntity input = new StringEntity(json.toString(),
							Charset.forName("UTF-8"));
					input.setContentType("application/json; charset=utf-8");
					postRequest.setEntity(input);

					HttpResponse response = httpClient.execute(postRequest);
					int resStatus = response.getStatusLine().getStatusCode();
					LOG.info("status = " + resStatus);
					if (resStatus == 403) {
						LOG.error("Permission error");
					} else if (resStatus == 200) {
						HttpEntity ent = response.getEntity();
						if (ent != null) {
							hasMoreData = true;
							String res = EntityUtils.toString(ent);
							JSONObject data = new JSONObject(res);
							JSONArray list = null;
							if (data.optJSONArray("d") != null) {
								list = data.getJSONArray("d");
							}
							if(list == null || list.length() == 0){
								LOG.info("No data found. Finished for :" + zip);
								zipDAO.markProcessed(zip, SOURCE);
								break;
							}
							LOG.info("Records found for zip = "+zip+" : "+list.length());
							for (int j = 0; j < list.length() - 1; j++) {
								JSONObject itemJson = list.getJSONObject(j);
								String facilityHtml = itemJson.getString("FacilityHtml");
								String clubsHtml = itemJson.getString("ClubsHtml");
								String clubsHtml2 = itemJson.getString("ClubsHtml2");
								int recordId = itemJson.getInt("ID");
								LOG.info("Processing zip = "+zip+" : "+j);
								parsePage(facilityHtml + clubsHtml + clubsHtml2, recordId, zip);
							}

							// LOG.info("Response from API - "+
							// list.toString());
							pageNumber++;
						}
					}
				}
			} catch (Exception e) {
				LOG.error("err",e);
			}
			// close connections
			try {
				dbConT.close();
			} catch (Exception e) {
				LOG.error("err",e);
			}
		}

		private void parsePage(String pagehtml, int ID, String zip)
				throws Exception {
			ProviderDAO providerDAO = new ProviderDAO(dbCon);

			HashMap<String, Object> resultMap = new HashMap<String, Object>();
			resultMap.put(Constants.KEY_SITE_NAME, SOURCE);
			// resultMap.put(Constants.KEY_PAGE_URL,SITE_HOME_URL);
			Document page = Jsoup.parse(pagehtml);
			String pageUrl = null;
			Element elem = page.getElementsByClass("MapPopTitle").first();
			String text = elem.text().trim();
			if (text != "") {
				resultMap.put(Constants.KEY_LOCATION, text);
			}
			elem = page.getElementsByClass("MapPopDetail").first();
			text = elem.html().trim();
			if (text != "") {
				String[] addr = text.split("<br>");
				if (addr.length > 0 && addr.length == 2) {
					resultMap.put(Constants.KEY_STREET_ADDRESS, addr[0]);
					String[] addr2 = addr[1].split(",");
					if (addr2.length > 0) {
						if (!addr2[1].equals("")) {
							resultMap.put(Constants.KEY_ZIP_CODE,
									addr2[1].trim());
						}
						String[] addr3 = addr2[0].split(" ");
						if (!addr3[addr3.length - 1].equals("")) {
							resultMap.put(Constants.KEY_STATE,
									addr3[addr3.length - 1]);
						}
						String city = "";
						for (int i = 0; i < addr3.length - 1; i++) {
							city += addr3[i] + " ";
						}
						resultMap.put(Constants.KEY_CITY, city);

					}
				} else if (addr.length > 2) {
					resultMap.put(Constants.KEY_STREET_ADDRESS, addr[0] + " "
							+ addr[1]);
					String[] addr2 = addr[2].split(",");
					if (addr2.length > 0) {
						if (!addr2[1].equals("")) {
							resultMap.put(Constants.KEY_ZIP_CODE, addr2[1]);
						}
						String[] addr3 = addr2[0].split(" ");
						if (!addr3[addr3.length - 1].equals("")) {
							resultMap.put(Constants.KEY_STATE,
									addr3[addr3.length - 1]);
						}
						String city = "";
						for (int i = 0; i < addr3.length - 1; i++) {
							city += addr3[i] + " ";
						}
						city = city.substring(0, city.length() - 4);
						LOG.info("city : " + city.length());
						resultMap.put(Constants.KEY_CITY, city);

					}
				}
			}
			Element contactElem = page
					.getElementsByClass("MapPopSubTitleRight").first();
			Element aElem = contactElem.getElementsByTag("a").first();
			String hrefElem = aElem.attr("href");
			String[] contactInfo = hrefElem.split("(,)|(')");
			resultMap.put(Constants.KEY_PROVIDER_NAME, contactInfo[3]);
			resultMap.put(Constants.KEY_CONTACT_NAME, contactInfo[6]);
			resultMap.put(Constants.KEY_PHONE, contactInfo[9]);
			resultMap.put(Constants.KEY_EMAIL, contactInfo[12]);

			Element websiteElem = page.getElementsByClass(
					"MapListTitleLinkBlock").first();
			Elements elemA = websiteElem.getElementsByTag("a");
			for (Element element : elemA) {
				if (element.attr("class").equals("MapPopSubTitleLink")) {
					hrefElem = element.attr("href");
					// System.out.println(hrefElem);
					if (hrefElem.startsWith("javascript:PopupWebsite")) {
						String[] weblink = hrefElem.split("'");
						pageUrl = weblink[1].trim();
						int x = weblink[1].indexOf("?");

						if (x == -1) {
							pageUrl += "?ID=" + ID;

						} else {
							pageUrl += "&ID=" + ID;
						}
						resultMap.put(Constants.KEY_WEBSITE, pageUrl);
						resultMap.put(Constants.KEY_PAGE_URL, pageUrl);

						// System.out.println(weblink[1]);
					}

				}

			}
			LOG.debug("result : "+resultMap.toString());
			if (pageUrl != null && !pageUrl.isEmpty()) {
				if((!providerDAO.isRecordExist(pageUrl))){
					providerDAO.addProvider(resultMap);
				}	
			}

		}

		private JSONObject prepareQueryParams(String zip, int pageNum) {
			HashMap<String, String> resultMap = new HashMap<String, String>();
			resultMap.put("bShowClubs", "true");
			resultMap.put("bShowPartners", "true");
			resultMap.put("iClubExLevel", "-1");
			resultMap.put("iClubRecLevel", "-1");
			resultMap.put("iMiles", "10");
			resultMap.put("iPageNum", String.valueOf(pageNum));
			resultMap.put("sZip", zip);
			resultMap.put("sCity", "");
			resultMap.put("sState", "");
			JSONObject json = new JSONObject(resultMap);
			return json;
		}
	}

}
