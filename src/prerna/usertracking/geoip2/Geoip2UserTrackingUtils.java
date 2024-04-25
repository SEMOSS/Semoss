package prerna.usertracking.geoip2;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.Location;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.usertracking.AbstractUserTrackingUtils;
import prerna.usertracking.IUserTracking;
import prerna.usertracking.UserTrackingDetails;
import prerna.util.Constants;
import prerna.util.Utility;

public class Geoip2UserTrackingUtils extends AbstractUserTrackingUtils {
	
	private static final Logger logger = LogManager.getLogger(Geoip2UserTrackingUtils.class);

	private static final CityResponse NULL_CR = new CityResponse(null, null, null, null, null, null, null, null, null, null);
	
	private static Geoip2UserTrackingUtils instance;
	
	private static DatabaseReader reader;
	
	private static String workdir = Utility.getBaseFolder();
	private static String folder = "GeoIp2Artifacts";
	private static String fileName = "GeoLite2-City.mmdb";
	private static String filePath = workdir + File.separator + folder + File.separator + fileName;
	
	public Geoip2UserTrackingUtils() {
		loadDatabaseReader();
	}
	
	private void loadDatabaseReader() {
		File database = new File(filePath);
		try {
			reader = new DatabaseReader.Builder(database).build();
		} catch (IOException e) {
			logger.error("Database reader object could not be loaded. IP Details will not be stored.");
			logger.error(Constants.STACKTRACE, e);
		}
	}
	
	public static IUserTracking getInstance() {
		if (instance != null) {
			return instance;
		}

		synchronized (Geoip2UserTrackingUtils.class) {
			if (instance == null) {
				try {
					instance = new Geoip2UserTrackingUtils();
				} catch (Exception e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return instance;
	}

	@Override
	public void registerLogin(String sessionId, String ip, User user, AuthProvider ap) {
		// try to load the reader
		if (reader == null) {
			loadDatabaseReader();
		}
		
		UserTrackingDetails utd;
		
		// if still no reader, return null tracking details
		if(reader == null) {
			utd = new UserTrackingDetails(
					ip,
					null,
					null,
					null,
					null,
					null
					);
		} else {
			try {
				InetAddress inet = InetAddress.getByName(ip);
				CityResponse cr = reader.tryCity(inet).orElse(NULL_CR);
				utd = this.cityResponseToUserTrackingDetails(cr, ip);
			} catch (IOException | GeoIp2Exception e) {
				logger.error("Error occurred while trying to find ip data.", e);
				utd = new UserTrackingDetails(
					ip,
					null,
					null,
					null,
					null,
					null
					);
			}
		}
		
		super.saveSession(sessionId, utd, user, ap);
	}
	
	private UserTrackingDetails cityResponseToUserTrackingDetails(CityResponse cr, String ip) {
		Location location = cr.getLocation();
		
		String lat;
		String lon;
		if (location == null) {
			lat = null;
			lon = null;
		} else {
			lat = location.getLatitude() != null ? location.getLatitude().toString() : null;
			lon = location.getLongitude() != null ? location.getLongitude().toString() : null;
		}
			
		String country = cr.getCountry() != null ? cr.getCountry().getName() : null;	
		String state = cr.getLeastSpecificSubdivision() != null ? cr.getLeastSpecificSubdivision().getName() : null;	
		String city = cr.getCity() != null ? cr.getCity().getName() : null;	
		
		UserTrackingDetails utd = new UserTrackingDetails(
					ip,
					lat,
					lon,
					country,
					state,
					city
				);
		
		logger.info("SessionTrackedDetails: {}", utd.toString());
		
		return utd;
	}

}
