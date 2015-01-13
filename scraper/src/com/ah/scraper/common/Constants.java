package com.ah.scraper.common;
 
import java.util.Collections;
import java.util.HashMap;
import java.util.Map; 

public class Constants {
	
	@SuppressWarnings("serial")
	static public final Map<String, String> SitesMap = Collections
			.unmodifiableMap(new HashMap<String, String>() {
				{
					put("acacamps", "ACACampScraper");
					put("activitytree", "ActivityTreeScraper");
					put("camppage", "CampPageScraper");		
					put("kidscamps", "KidsCampScraper");
					put("summercamps", "SummerCampScrapper");
					put("mysummercamps", "MySummerCampScraper");
					put("aisne", "AisneScraper");
					put("usaswimming", "UsaSwimmingScraper");
					put("savvysource", "SavvySourceScraper");
					put("publiclibraries","PublicLibrariesScraper");
					put("activityrocket","ActivityRocketScraper");
					put("yellowpages", "YelloPagesScraper");
					put("bayareaparentkidscamps", "BayAreaParentKidsCampsScraper");
					put("ymca", "YmcaScraper");
				}
			});	 
	
	public static enum STATUS {
		NEW, EXPORT_ERROR, EXPORT_SUCCESS, ;
	}
	//prod
	final public static String UPLOAD_URL = "https://www.activityhero.com/api/providers/import.json";
	final public static String AUTH_USER = "import@activityhero.com"; 
	final public static String AUTH_PASSWORD = "V9v75yLMGd";
	
	//dev
	//final public static String UPLOAD_URL = "https://ah-wli.herokuapp.com/api/providers/import.json"; 
	//final public static String AUTH_USER = "import@activityhero.com"; 
	//final public static String AUTH_PASSWORD = "blueredgreen"; 
	
	final public static String AUTH_URL = UPLOAD_URL;
	final public static int REQUEST_TIMEOUT = 1000 * 60;
	final public static String USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:33.0) Gecko/20100101 Firefox/33.0";
	final public static int MAX_RETRY_COUNT = 10;

	final public static int DELAY_IN_RETRY = 1; // minutes

	final static public boolean SHOULD_OVERRIDE_RECORDS = false;		
	
	final public static String KEY_SITE_NAME = "source_site";
	final public static String KEY_PROVIDER_NAME = "provider_name";
	final public static String KEY_LOGO_URL = "logo_url";
	final public static String KEY_PHOTO_URL = "photo_url";
	final public static String KEY_WEBSITE = "website";
	final public static String KEY_STREET_ADDRESS = "street_address";
	final public static String KEY_CITY = "city";
	final public static String KEY_ZIP_CODE = "zip_code";
	final public static String KEY_PHONE = "phone";
	final public static String KEY_DESCRIPTION = "description";
	final public static String KEY_CONTACT_NAME = "contact_name";
	final public static String KEY_EMAIL = "email";
	final public static String KEY_STATE = "state";
	final public static String KEY_ID = "id";
	final public static String KEY_PAGE_URL = "page_url";
	final public static String KEY_LOCATION = "location";

	final public static String KEY_HAS_CLASS = "has_class";
	final public static String KEY_HAS_BIRTHDAY_PARTY = "has_birthday_party";
	final public static String KEY_ACTIVITY_TYPE = "activity_type";
	final public static String KEY_REG_TYPE = "registration_type";
	final public static String KEY_FB_URL = "fb_url";
	final public static String KEY_NOTES = "notes";

	final public static String KEY_ALL_PROGRAMS = "all_programs";
	final public static String KEY_HAS_CAMP = "has_camp";

	final public static String KEY_FROM_AGE = "from_age";
	final public static String KEY_TO_AGE = "to_age";
	final public static String KEY_FROM_GRADE = "from_grade";
	final public static String KEY_TO_GRADE = "to_grade";
	final public static String KEY_FROM_PRICE = "from_price";
	final public static String KEY_TO_PRICE = "to_price";
	final public static String KEY_ACTIVITIES = "activities";
	final public static String KEY_PRICE_TYPE = "price_type";

	final public static String KEY_PROGRAM_NAME = "program_name";
	final public static String KEY_PROGRAM_FOR = "program_for";
	final public static String KEY_PROGRAM_TYPE = "type";
	final public static String KEY_PROGRAM_TRANSPORTATION = "transportation";
	final public static String KEY_PROGRAM_ID = "program_id";

	final public static String KEY_IS_EXTERNAL_URL = "is_external";

	public static final String PROGRAM_FOR_COED = "Coed";

	public static final String PROGRAM_FOR_GIRLS = "Only Girls";

	public static final String PROGRAM_FOR_BOYS = "Only Boys";
	
	public static final String EXTERNAL_URL_YES = "Y";
	public static final String EXTERNAL_URL_NO = "N";
	
	
	// Activity
	
	final public static String KEY_PROVIDER_ID = "provider_id";
	final public static String KEY_ACTIVITY_NAME = "activity_name";
	final public static String KEY_BOYS_ONLY = "boys_only";
	final public static String BOYS_ONLY = "1";
	final public static String KEY_GIRLS_ONLY = "girls_only";
	final public static String GIRLS_ONLY = "1";
	
	
	

}
