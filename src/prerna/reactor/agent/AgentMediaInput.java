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
package prerna.reactor.agent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Normalized image/media descriptor for insight-backed uploaded assets
 * forwarded from the frontend.
 */
public record AgentMediaInput(String attachmentId, String promptId, String fileName, String mimeType, String path) {

	@SuppressWarnings("unchecked")
	public static List<AgentMediaInput> fromUnknown(Object value) {
		List<AgentMediaInput> result = new ArrayList<>();
		if (!(value instanceof List<?>)) {
			return result;
		}

		for (Object item : (List<?>) value) {
			if (!(item instanceof Map<?, ?>)) {
				continue;
			}

			Map<String, Object> record = (Map<String, Object>) item;
			String attachmentId = asTrimmedString(record.get("attachmentId"));
			String promptId = asTrimmedString(record.get("promptId"));
			String fileName = asTrimmedString(record.get("fileName"));
			String mimeType = asTrimmedString(record.get("mimeType"));
			String path = asTrimmedString(record.get("path"));

			if (attachmentId == null || promptId == null || fileName == null || mimeType == null || path == null) {
				continue;
			}

			result.add(new AgentMediaInput(attachmentId, promptId, fileName, mimeType, path));
		}

		return result;
	}

	private static String asTrimmedString(Object value) {
		if (value == null) {
			return null;
		}
		String stringValue = String.valueOf(value).trim();
		return stringValue.isEmpty() ? null : stringValue;
	}
}
