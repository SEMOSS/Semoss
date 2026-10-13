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
package prerna.reactor.automation.run;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.om.Insight;

/**
 * Builds governed file manifests without exposing server filesystem paths.
 */
final class AutomationFileManifestService {

	private AutomationFileManifestService() {
	}

	static Map<String, Object> capture(Insight insight, Map<String, Object> result, String nodeId) {
		Object rawFiles = result.get("files");
		if (!(rawFiles instanceof List<?> files) || files.isEmpty()) {
			throw new IllegalStateException("Storage node '" + nodeId + "' returned no governed files.");
		}
		Path root = Path.of(insight.getInsightFolder()).toAbsolutePath().normalize();
		List<Map<String, Object>> manifests = new ArrayList<>(files.size());
		for (Object rawFile : files) {
			String locator = String.valueOf(rawFile).replace('\\', '/');
			String relative = locator.startsWith("/") ? locator.substring(1) : locator;
			Path file = root.resolve(relative).normalize();
			if (!file.startsWith(root) || !Files.isRegularFile(file)) {
				throw new IllegalStateException(
						"Storage node '" + nodeId + "' produced an invalid governed file locator.");
			}
			Map<String, Object> manifest = new LinkedHashMap<>();
			manifest.put("space", "INSIGHT");
			manifest.put("path", "/" + root.relativize(file).toString().replace('\\', '/'));
			try {
				manifest.put("mediaType", Files.probeContentType(file));
				manifest.put("byteCount", Files.size(file));
				manifest.put("checksum", checksum(file));
			} catch (IOException e) {
				throw new IllegalStateException(
						"Unable to capture downloaded file metadata for storage node '" + nodeId + "'.", e);
			}
			manifests.add(manifest);
		}
		Map<String, Object> captured = new LinkedHashMap<>(result);
		captured.put("files", manifests);
		captured.put("file", manifests.size() == 1 ? manifests.get(0) : null);
		captured.remove("filePath");
		return captured;
	}

	private static String checksum(Path file) throws IOException {
		MessageDigest digest;
		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 is unavailable.", e);
		}
		try (InputStream input = Files.newInputStream(file)) {
			byte[] buffer = new byte[64 * 1024];
			int read;
			while ((read = input.read(buffer)) >= 0) {
				if (read > 0) {
					digest.update(buffer, 0, read);
				}
			}
		}
		return HexFormat.of().formatHex(digest.digest());
	}
}
