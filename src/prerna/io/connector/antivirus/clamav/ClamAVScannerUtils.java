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
package prerna.io.connector.antivirus.clamav;

import java.io.InputStream;
import java.nio.file.FileSystems;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.io.connector.antivirus.IVirusScanner;
import prerna.util.Utility;
import xyz.capybara.clamav.ClamavClient;
import xyz.capybara.clamav.Platform;
import xyz.capybara.clamav.commands.scan.result.ScanResult;

public final class ClamAVScannerUtils implements IVirusScanner {

	private static final Logger classLogger = LogManager.getLogger(ClamAVScannerUtils.class);

	public static final String CLAMAV_SCANNING_PORT = "CLAMAV_SCANNING_PORT";
	public static final String CLAMAV_SCANNING_ADDRESS = "CLAMAV_SCANNING_ADDRESS";
	public static final String CLAMAV_SCANNING_FS = "CLAMAV_SCANNING_FS";

	private static volatile ClamAVScannerUtils instance;
	private ClamavClient client;

	private ClamAVScannerUtils() throws Exception {
		int port = Optional.ofNullable(getVirusScanningPort()).orElseThrow(() -> new Exception("Port cannot be null"));

		String address = Optional.ofNullable(getVirusScanningAddress())
				.orElseThrow(() -> new Exception("Address cannot be null"));

		Platform platform = Optional.ofNullable(getVirusScanningFileSystem())
				.orElseThrow(() -> new Exception("Platform cannot be null"));

		classLogger.info("address: {} port: {} platform: {}", address, port, platform);

		this.client = new ClamavClient(address, port, platform);
	}

	public static IVirusScanner getInstance() {
		if (instance != null) {
			return instance;
		}

		if (instance == null) {
			synchronized (ClamAVScannerUtils.class) {
				if (instance == null) {
					try {
						instance = new ClamAVScannerUtils();
					} catch (Exception e) {
						classLogger.error(
								"Failed to initialize ClamAV scanner singleton. Check properties {}='{}', {}='{}', {}='{}'. Virus scanning will be unavailable.",
								CLAMAV_SCANNING_ADDRESS, Utility.getDIHelperProperty(CLAMAV_SCANNING_ADDRESS),
								CLAMAV_SCANNING_PORT, Utility.getDIHelperProperty(CLAMAV_SCANNING_PORT),
								CLAMAV_SCANNING_FS, Utility.getDIHelperProperty(CLAMAV_SCANNING_FS), e);
					}
				}
			}
		}

		return instance;
	}

	@Override
	public Map<String, Collection<String>> getViruses(String name, InputStream is) {
		ScanResult sr = this.client.scan(is);

		if (sr instanceof ScanResult.OK) {
			return new HashMap<String, Collection<String>>();
		} else {
			return ((ScanResult.VirusFound) sr).getFoundViruses();
		}
	}

	/**
	 * 
	 * @return
	 */
	private static String getVirusScanningAddress() {
		return Utility.getDIHelperProperty(CLAMAV_SCANNING_ADDRESS);
	}

	/**
	 * 
	 * @return
	 */
	private static Integer getVirusScanningPort() {
		String virusScanning = Utility.getDIHelperProperty(CLAMAV_SCANNING_PORT);
		if (virusScanning == null) {
			// default configuration is false
			return null;
		}

		return Integer.valueOf(virusScanning);
	}

	/**
	 * 
	 * @return
	 */
	public static Platform getVirusScanningFileSystem() {
		String platform = Utility.getDIHelperProperty(CLAMAV_SCANNING_FS);

		if ("WINDOWS".equalsIgnoreCase(platform)) {
			return Platform.WINDOWS;
		} else if ("UNIX".equalsIgnoreCase(platform)) {
			return Platform.UNIX;
		} else {
			String sep = FileSystems.getDefault().getSeparator();
			if (sep.equals("/")) {
				return Platform.UNIX;
			} else if (sep.equals("\\")) {
				return Platform.WINDOWS;
			} else {
				return null;
			}
		}
	}
}
