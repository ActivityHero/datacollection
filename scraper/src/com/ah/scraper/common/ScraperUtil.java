package com.ah.scraper.common;


import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ScraperUtil {
	
	static public void log(Object s) {
		System.out.println(s);
	}

	static public String tabbedStrFromMap(HashMap<String, Object> map, String key) {
		String sb = "";
		if(map == null) return sb;
		
		Object act = map.get(key);
		if (act != null) {
			@SuppressWarnings("unchecked")
			ArrayList<String> activities = (ArrayList<String>) act;
			for (String s : activities) {
				sb = sb + s;
				if (!s.equals(activities.get(activities.size() - 1))) {
					sb = sb + "\t";
				}
			}
		}
		return sb.toString();
	}
	
	

	static public boolean validEmail(String str) {

		String EMAIL_REGEX = "^[\\w-_\\.+]*[\\w-_\\.]\\@([\\w]+\\.)+[\\w]+[\\w]$";
		Boolean b = str.matches(EMAIL_REGEX);
		return b;

	}	
	
	public static Map<String, List<String>> splitQuery(URL url)
			throws UnsupportedEncodingException {
		final Map<String, List<String>> query_pairs = new LinkedHashMap<String, List<String>>();
		final String[] pairs = url.getQuery().split("&");
		for (String pair : pairs) {
			final int idx = pair.indexOf("=");
			final String key = idx > 0 ? URLDecoder.decode(
					pair.substring(0, idx), "UTF-8") : pair;
			if (!query_pairs.containsKey(key)) {
				query_pairs.put(key, new LinkedList<String>());
			}
			final String value = idx > 0 && pair.length() > idx + 1 ? URLDecoder
					.decode(pair.substring(idx + 1), "UTF-8") : null;
			query_pairs.get(key).add(value);
		}
		return query_pairs;
	}	
	
	static public String locationFromMap(HashMap<String, Object> map) {
		if(map.containsKey(Constants.KEY_LOCATION))
			return (String) map.get(Constants.KEY_LOCATION);
		String street = (String) map.get(Constants.KEY_STREET_ADDRESS);
		if (street == null)
			street = "";
		String city = (String) map.get(Constants.KEY_CITY);
		if (city == null)
			city = "";
		String state = (String) map.get(Constants.KEY_STATE);
		if (state == null)
			state = "";
		String zip = (String) map.get(Constants.KEY_ZIP_CODE);
		if (zip == null)
			zip = "";

		String location = "";
		if (!street.isEmpty()) {
			location = location + street;
			location = location + "\n";
		}
		if (!city.isEmpty()) {
			location = location + city;
			location = location + ", ";
		}
		if (!state.isEmpty()) {
			location = location + state;
			location = location + " ";
		}
		if (!zip.isEmpty()) {
			location = location + zip;
		}
		return location;
	}	
	
	static public void sleep(){
//		try {
//			Thread.sleep(2);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
}
