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
import prerna.util.DIHelper;

public class Geoip2UserTrackingUtils extends AbstractUserTrackingUtils {
	
	private static final Logger logger = LogManager.getLogger(Geoip2UserTrackingUtils.class);

	private static final CityResponse NULL_CR = new CityResponse(null, null, null, null, null, null, null, null, null, null);
	
	private static Geoip2UserTrackingUtils instance;
	
	public Geoip2UserTrackingUtils() {
		
	}
	
	public static IUserTracking getInstance() {
		if (instance != null) {
			return instance;
		} 

		
		synchronized (Geoip2UserTrackingUtils.class) {
			if(instance == null) {
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
		String workdir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String folder = "GeoIp2Artifacts";
		String fileName = "GeoLite2-City.mmdb";
		
		String filePath = workdir + File.separator + folder + File.separator + fileName;

		File database = new File(filePath);
		
		UserTrackingDetails utd;
		try {
			DatabaseReader reader = new DatabaseReader.Builder(database).build();
			InetAddress inet = InetAddress.getByName(ip);
			CityResponse cr = reader.tryCity(inet).orElse(NULL_CR);
			utd = this.cityResponseToUserTrackingDetails(cr, ip);
		} catch (IOException | GeoIp2Exception e) {
			logger.error("Error occured while trying to find ip data.", e);
			utd = new UserTrackingDetails(
				ip,
				null,
				null,
				null,
				null,
				null
				);
		}
		
		super.saveSession(sessionId, utd, user, ap);
	}
	
	/**
	 * 
	 * @param cr
	 * @param ip
	 * @return
	 */
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
