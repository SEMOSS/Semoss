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
package prerna.reactor.automation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Base64;

import prerna.om.Insight;
import prerna.reactor.automation.utils.AutomationExecutionUtils;
import prerna.reactor.automation.utils.PixelExecutionUtils;
import prerna.util.AssetUtility;

/**
 * Reads automation step source from its project asset path and persists updates through the
 * versioned application-asset save reactor.
 */
final class AutomationStepSourceService {

	private AutomationStepSourceService() {
	}

	static boolean sourceExists(String projectId, String stepRef) {
		return Files.exists(resolveStepPath(projectId, stepRef), LinkOption.NOFOLLOW_LINKS);
	}

	static String readSource(String projectId, String stepRef) {
		Path stepPath = resolveStepPath(projectId, stepRef);
		if (Files.isSymbolicLink(stepPath) || !Files.isRegularFile(stepPath)) {
			throw new IllegalArgumentException("Automation step source does not exist: " + stepRef);
		}
		try {
			return Files.readString(stepPath, StandardCharsets.UTF_8);
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to read automation step source: " + stepRef, e);
		}
	}

	static void saveSource(Insight insight, String projectId, String stepRef, String source, String comment) {
		String encodedSource = Base64.getEncoder().encodeToString(source.getBytes(StandardCharsets.UTF_8));
		String pixel = "SaveAppAssetsBase64(project=" + pixelStringList(projectId)
				+ ", filePath=" + pixelStringList(stepRef)
				+ ", content=" + pixelStringList(encodedSource)
				+ ", comment=" + pixelStringList(comment) + ");";
		PixelExecutionUtils.runAndCollect(insight, pixel);
	}

	private static Path resolveStepPath(String projectId, String stepRef) {
		Path assetsFolder = Path.of(AssetUtility.getProjectAssetsFolder(projectId)).toAbsolutePath().normalize();
		Path stepPath = assetsFolder.resolve(stepRef).normalize();
		if (!stepPath.startsWith(assetsFolder)) {
			throw new IllegalArgumentException("Automation step source must be inside the project assets folder.");
		}
		return stepPath;
	}

	private static String pixelStringList(String value) {
		return "[" + AutomationExecutionUtils.GSON.toJson(value) + "]";
	}
}
