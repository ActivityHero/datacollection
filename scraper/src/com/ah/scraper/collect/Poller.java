package com.ah.scraper.collect;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ah.scraper.common.Constants;
import com.ah.scraper.common.ScraperUtil;
import com.ah.scraper.scrapers.IScraper;

public class Poller{
	private static Logger LOG = LoggerFactory.getLogger(Poller.class);
	private int minThreads = 1;
	private int maxThreads = 10;
	private int queueSize = 1000;
	ExecutorService executorService;
	public Poller(){
		executorService =
				  new ThreadPoolExecutor(
				    minThreads, // core thread pool size
				    maxThreads, // maximum thread pool size
				    1, // time to wait before resizing pool
				    TimeUnit.MINUTES, 
				    new ArrayBlockingQueue<Runnable>(queueSize, true),
				    new ThreadPoolExecutor.CallerRunsPolicy());
	}
	public void launch(ArrayList<String> sites){ 
		for(String site : sites){
			ScraperUtil.log("Site to be started is -"+site);
			try {
				LOG.debug(Constants.SitesMap.get(site));
				Class<?> obj = Class.forName("com.ah.scraper.scrapers."+Constants.SitesMap.get(site));
 				executorService.submit((IScraper)obj.newInstance()); 
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				LOG.error("err",e);
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				LOG.error("err",e);
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				LOG.error("err",e);
			}
			
		} 
	}
	
	@Override
	protected void finalize() throws Throwable {
		if (executorService != null) {
			executorService.shutdown();
		}
		super.finalize();
	}
}
