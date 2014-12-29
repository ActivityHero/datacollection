package com.ah.scraper.scrapers;

import java.io.FileWriter;
import java.util.ArrayList;


import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVWriter;

import com.ah.scraper.common.Constants;


public class PublicLibrariesScraper implements IScraper {

	private static Logger LOG = LoggerFactory.getLogger(PublicLibrariesScraper.class);
	public static String SITE_HOME_URL = "http://www.publiclibraries.com/california.htm";
    public static String SOURCE = "publiclibraries";
    
   
	public void run() {
		try { 
			LOG.info("PublicLibrariesScraper run called");
			this.startScraping();
		} catch (Exception e) { 
			LOG.error("err",e);
		}
	}

	public void startScraping() throws Exception {
		CSVWriter writer = new CSVWriter(new FileWriter("d:/logs/library.csv"));
		Document page = Jsoup.connect(SITE_HOME_URL).timeout(Constants.REQUEST_TIMEOUT).get();
		if(page != null){
			Elements tableElem = page.getElementsByTag("table");
			if(tableElem != null){
				for (Element table : tableElem) {
					if(table != null){
						Elements trElem = table.getElementsByTag("tr");
						if(trElem != null){
							
							boolean heading = true;
							for (Element tr : trElem) {
								ArrayList<String> result = new ArrayList<String>();
								if(heading){
									heading = false;
								}else{
									Elements tdElem = tr.getElementsByTag("td");
									int i = 1;
									if(tdElem.size()==5){
										for (Element td : tdElem) {
											String text = td.text().trim();
											if(!text.isEmpty()){
												result.add(text);
											}else{
												result.add("");
											}
										}
										String[] detail = result.toArray(new String[] {});
										writer.writeNext(detail);
										
									}else if(tdElem.size()==6){
										for (Element td : tdElem) {
											if(i==6){
												Element aElem = td.getElementsByTag("a").first();
												if(aElem != null){
													String text = aElem.absUrl("href").trim();
													if(text.contains("www.") || text.contains(".html") || text.contains(".php")){
														result.add("");
														result.add(text);
													}else{
														String[] mail = text.split(":");
														result.add(mail[1]);
													}
													
												}
											}else{
												String text = td.text().trim();
												if(!text.isEmpty()){
													result.add(text);
												}else{
													result.add("");
												}
											}
											i++;
										}
										String[] detail = result.toArray(new String[] {});
										writer.writeNext(detail);
									}
									writer.flush();
								}
						
								LOG.info("result : "+result.toString());
							}
						}
					}
				}
				writer.close();
			}
		}
	}

}
