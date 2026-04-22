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
package prerna.io.connector.antivirus.tika;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.Tika;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;

import prerna.io.connector.antivirus.IVirusScanner;

public class ApacheTikaScannerUtils implements IVirusScanner {

	private static final Logger classLogger = LogManager.getLogger(ApacheTikaScannerUtils.class);

	public ApacheTikaScannerUtils() {

	}

	@Override
	public Map<String, Collection<String>> getViruses(String name, InputStream is) {
		Map<String, Collection<String>> retMap = new HashMap<>();

		Tika tika = new Tika();
		Metadata metadata = new Metadata();
		try {
			String detectedType = tika.detect(is, metadata);
			classLogger.info("Predicted {} has type {}", name, detectedType);
			if (isSubtypeOfMsDownload(detectedType)) {
				Collection<String> allIssues = new TreeSet<>();
				retMap.put(name, allIssues);
				allIssues.add(detectedType);
			}
		} catch (IOException e) {
			classLogger.error("Failed to detect file type for '{}'.", name, e);
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
					classLogger.error("Failed to close input stream after virus scan for '{}'.", name, e);
				}
			}
		}

		return retMap;
	}

	private static boolean isSubtypeOfMsDownload(String mimeType) {
		MediaType mediaType = MediaType.parse(mimeType);
		MediaType baseType = mediaType.getBaseType();
		return baseType.equals(MediaType.parse("application/x-msdownload"));
	}

}
