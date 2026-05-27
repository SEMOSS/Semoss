/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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
import prerna.util.Utility;

public final class Geoip2UserTrackingUtils extends AbstractUserTrackingUtils {

	private static final Logger classLogger = LogManager.getLogger(Geoip2UserTrackingUtils.class);

	private static final CityResponse NULL_CR = new CityResponse(null, null, null, null, null, null, null, null, null,
			null);

	private static volatile Geoip2UserTrackingUtils instance;
	private static volatile DatabaseReader reader;

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
			classLogger.error("Failed to load GeoIP2 database reader from " + database.getAbsolutePath()
					+ ". IP details will not be stored for user tracking sessions.", e);
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
						classLogger.error("Failed to initialize Geoip2UserTrackingUtils singleton using database path "
								+ filePath + ". User tracking will continue without GeoIP enrichment.", e);
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
		if (reader == null) {
			utd = new UserTrackingDetails(ip, null, null, null, null, null);
		} else {
			try {
				InetAddress inet = InetAddress.getByName(ip);
				CityResponse cr = reader.tryCity(inet).orElse(NULL_CR);
				utd = this.cityResponseToUserTrackingDetails(cr, ip);
			} catch (IOException | GeoIp2Exception e) {
				classLogger.error("Error occurred while trying to find ip data.", e);
				utd = new UserTrackingDetails(ip, null, null, null, null, null);
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

		UserTrackingDetails utd = new UserTrackingDetails(ip, lat, lon, country, state, city);

		classLogger.info("SessionTrackedDetails: {}", utd.toString());
		return utd;
	}

}
