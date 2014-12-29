package com.ah.scraper.export;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ah.scraper.dto.ProviderDTO;

public class ReadCSV {
	
	private static Logger LOG = LoggerFactory.getLogger(ReadCSV.class);
	private String fileName;
	private ArrayList<String> columns;
	public ReadCSV(String name){
		this.fileName = name;
	}
	
	private String getFilePath(){
		return "../scraper/csv/"+fileName+".csv";
	}
	
	public ArrayList<ProviderDTO> parse() {
		BufferedReader br = null;
		ArrayList<ProviderDTO> pDtos = null;
		try {
			Reader in = new FileReader(getFilePath());
			Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(in);
			columns = new ArrayList<String>();
			int lineCount = 0;
			pDtos = new ArrayList<ProviderDTO>();
			for (CSVRecord record : records) {
				//it's column name line
				if(lineCount == 0){
					Iterator<String> itr = record.iterator();
					while(itr.hasNext()){
						columns.add(itr.next());
					}
				}else{
					ProviderDTO pDto = getProviderDTOFromArray(record);
					if(pDto != null){
						pDtos.add(pDto);
					}
				}
			
				lineCount++;
			}
			

		} catch (FileNotFoundException e) {
			LOG.error("err",e);
		} catch (IOException e) {
			LOG.error("err",e);
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					LOG.error("err",e);
				}
			}
		}
		
		return pDtos;
	}
	
	private ProviderDTO getProviderDTOFromArray(CSVRecord record){
		ProviderDTO pDto = new ProviderDTO();
		int i = 0;
		StringBuffer activities = new StringBuffer("");
		Iterator<String> itr = record.iterator();
		while(itr.hasNext()){
			String colName = columns.get(i);
			String item = itr.next();
			i++;
			if(item == null){
				continue;
			}
			item = item.trim();
			if(item.equals("Yes") || item.equals("yes") || item.equals("YES"))
				item = "Y";
			if(item.equals("No") || item.equals("no") || item.equals("NO"))
				item = "N";
			
			if(colName.equals("provider_name")){
				pDto.setProvider_name(item);
			}else if(colName.equals("website")){
				pDto.setWebsite(item);
			}else if(colName.equals("email")){
				pDto.setEmail(item);
			}else if(colName.equals("street_address")){
				pDto.setStreet(item);
			}else if(colName.equals("city")){
				pDto.setCity(item);
			}else if(colName.equals("state")){
				pDto.setState(item);
			}else if(colName.equalsIgnoreCase("Zipcode")){
				pDto.setZip(item);
			}else if(colName.equals("phone")){
				pDto.setPhone(item);
			}else if(colName.equals("description")){
				pDto.setDescription(item);
			}else if(colName.equals("from_age")){
				try{
					pDto.setFrom_age(getAgeInMonths(item));
				}catch(Exception e){
					LOG.error("err",e);
				}
			}else if(colName.equals("to_age")){
				try{
					pDto.setTo_age(getAgeInMonths(item));
				}catch(Exception e){
					LOG.error("err",e);
				}
			}else if(colName.equals("has_class")){
				pDto.setHas_class(item);
			}else if(colName.equals("has_camp")){
				pDto.setHas_camp(item);
			}else if(colName.equals("has_birthday_party")){
				pDto.setHas_birthday_party(item);
			}else if(colName.startsWith("activity type")){
				activities.append(item+"\t");
			}else if(colName.equalsIgnoreCase("Starting price")){
				try{
					double price = -1.00;
					if(!item.isEmpty()){
						price = Double.parseDouble(item);
					}
					pDto.setFrom_price(price);
				}catch(Exception e){
					pDto.setFrom_price(-1.00);
					LOG.error("err",e);
				}
			}else if(colName.startsWith("price_type")){
				pDto.setPrice_type(item);
			}else if(colName.startsWith("Registration Type")){
				pDto.setRegistration_type(item);
			}else if(colName.equals("Facebook page")){
				pDto.setFacebook_url(item);
			}else if(colName.equals("logo_url")){
				pDto.setLogo_url(item);
			}else if(colName.equals("photo_url")){
				pDto.setPhoto_url(item);
			}else if(colName.equals("location_name")){
				pDto.setLocation_name(item);
			}else if(colName.equals("from_grade")){
				try{
					pDto.setFrom_grade(getGrade(item));
				}catch(Exception e){
					LOG.error("err",e);
				}
			}else if(colName.equals("to_grade")){
				try{
					pDto.setTo_grade(getGrade(item));
				}catch(Exception e){
					LOG.error("err",e);
				}
			}else if(colName.equalsIgnoreCase("NOTES")){
				pDto.setNotes(item);
			}else if(colName.equalsIgnoreCase("hot lead")){
				pDto.setHotLead(item);
			}
			//we don't have to price/alt price info
			pDto.setTo_price(-1.00);
		}
		if(activities.length() > 0){
			pDto.setActivities(activities.toString().trim());
		}
		pDto.setCreated(Calendar.getInstance().getTimeInMillis());
		return pDto;
	}
	
	private String getGrade(String grade){
		if(grade.equalsIgnoreCase("K"))
			grade = "0";
		else if(grade.equalsIgnoreCase("pre-k") || grade.equalsIgnoreCase("Pre K"))
			grade = "-1";
		else if(grade.equalsIgnoreCase("pre-school"))
			grade = "-2";
		
		return grade;
	}
	private int getAgeInMonths(String age){
		int ageInMonths = -1;
		if(age.isEmpty())
			return ageInMonths;
		else if(age.equalsIgnoreCase("adult"))
			age = "99";
		try{
			Float a = Float.parseFloat(age);
			a = a * 12;
			ageInMonths = a.intValue();
		}catch(Exception e){
			LOG.error("err",e);
		}
		return ageInMonths;
	}
	
}
