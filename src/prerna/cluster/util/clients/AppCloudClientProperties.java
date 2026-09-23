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
package prerna.cluster.util.clients;

import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Utility;

/**
 * Resolves cluster configuration values, checking environment variables first
 * and falling back to DIHelper properties. Also owns the container's identity
 * ({@link #getContainerIp()}), generating a stable per-process id when one is
 * not supplied.
 */
public class AppCloudClientProperties {

	private static final Logger classLogger = LogManager.getLogger(AppCloudClientProperties.class);

	private static volatile Map<String, String> variables = System.getenv();
	private static volatile String generatedHostIp = null;

	public static final String HOST_IP = "HOST_IP";

	private AppCloudClientProperties() {

	}

	/**
	 * @return a new {@link AppCloudClientProperties} accessor
	 */
	public static AppCloudClientProperties build() {
		return new AppCloudClientProperties();
	}

	/**
	 * Resolves a config value, checking DIHelper (RDF_Map) properties first and
	 * falling back to environment variables if not found. Each source is checked
	 * with the key as-is, upper-cased, then lower-cased. Returns {@code null} if
	 * the value is absent or blank in both.
	 *
	 * @param key
	 * @return
	 */
	public String get(String key) {
		// DIHelper (RDF_Map) first
		String val = Utility.getDIHelperProperty(key);
		if (val != null && !(val = val.trim()).isEmpty()) {
			return val;
		}
		// give benefit of the doubt..
		val = Utility.getDIHelperProperty(key.toUpperCase());
		if (val != null && !(val = val.trim()).isEmpty()) {
			return val;
		}
		val = Utility.getDIHelperProperty(key.toLowerCase());
		if (val != null && !(val = val.trim()).isEmpty()) {
			return val;
		}

		// fall back to environment variables
		val = AppCloudClientProperties.variables.get(key);
		if (val != null && !(val = val.trim()).isEmpty()) {
			return val;
		}
		// give benefit of the doubt..
		val = AppCloudClientProperties.variables.get(key.toUpperCase());
		if (val != null && !(val = val.trim()).isEmpty()) {
			return val;
		}
		val = AppCloudClientProperties.variables.get(key.toLowerCase());
		if (val != null && !(val = val.trim()).isEmpty()) {
			return val;
		}

		// no luck...
		return null;
	}

	/**
	 * Get a unique container IP. This will default to the HOST_IP in env variables
	 * or DIHelper. If the IP is not defined, it will generate a new one.
	 * 
	 * @return
	 */
	public String getContainerIp() {
		String hostIp = get(HOST_IP);
		if (hostIp != null) {
			return hostIp;
		}

		// not found - reuse or create a new generated value
		if (generatedHostIp != null) {
			return generatedHostIp;
		}

		synchronized (AppCloudClientProperties.class) {
			if (generatedHostIp == null) {
				generatedHostIp = "host_" + UUID.randomUUID().toString();
				classLogger.info("Host IP is not set. Setting new value {}", generatedHostIp);
			}
		}

		return generatedHostIp;
	}

}
