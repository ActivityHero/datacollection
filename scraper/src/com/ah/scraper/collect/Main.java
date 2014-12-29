package com.ah.scraper.collect; 
 
import java.util.ArrayList; 
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ah.scraper.common.Constants; 
import com.ah.scraper.common.ScraperUtil; 

public class Main {
	 
	private static Logger LOG = LoggerFactory.getLogger(Main.class);
	Poller p = new Poller();

	/**
	 * @param args
	 * java Main <site1-key> <site2-key> 
	 * All params are optional
	 */
	public static void main(String[] args) {  
			Main m = new Main();
			
			if(m.validateEnv()){
				ArrayList<String>sites = m.rationalizeInput(args);
				m.poll(sites);
			} 
	} 

	public boolean validateEnv(){
		ScraperUtil.log("Scraper main - Max memory: "
				+ ((Runtime.getRuntime().maxMemory()) / 1024) / 1024 + "M"); 
		LOG.info("Scraper main - Max memory: {} M",((Runtime.getRuntime().maxMemory()) / 1024) / 1024);
		
		//TODO:
		//check for db connection
		//check for internet connection
		return true;
	}
	
	public ArrayList<String> rationalizeInput(String[] args){
		ArrayList<String> sites = new ArrayList<String>();
		//first two strings are simply java Main, so start looking after that
		int i = 0;
		for(;i<args.length;i++){
			sites.add(args[i]);
		}
		//if there is no parameter add all by default
		if(i == 0){
			for (Map.Entry<String, String> entry : Constants.SitesMap.entrySet()) {
				sites.add(entry.getKey()); 
			}			
		} 
		return sites;
	} 
	
	public void poll(ArrayList<String> sites){
		p.launch(sites);
	}
	
}

