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
package prerna.reactor.shortcuts.conductor.oss;

import java.util.List;
import java.util.Map;

public class PixelBuilder {
	public static String toPixel(Object obj) {

		if (obj == null) {
			return "null";
		}

		// Map - {key=value,...}
		if (obj instanceof Map) {

			StringBuilder sb = new StringBuilder("{");
			Map<?, ?> map = (Map<?, ?>) obj;

			for (Map.Entry<?, ?> entry : map.entrySet()) {

				String key = entry.getKey().toString();
				Object value = entry.getValue();

				sb.append(key).append("=").append(toPixel(value)).append(",");
			}

			// remove last comma
			if (sb.charAt(sb.length() - 1) == ',') {
				sb.deleteCharAt(sb.length() - 1);
			}

			sb.append("}");
			return sb.toString();
		}

		// List - ["a","b",10]
		if (obj instanceof List) {

			StringBuilder sb = new StringBuilder("[");
			List<?> list = (List<?>) obj;

			for (Object item : list) {
				sb.append(toPixel(item)).append(",");
			}

			if (sb.charAt(sb.length() - 1) == ',') {
				sb.deleteCharAt(sb.length() - 1);
			}

			sb.append("]");
			return sb.toString();
		}

		// String - "value"
		if (obj instanceof String) {
			return "\"" + escape((String) obj) + "\"";
		}

		// Boolean / Number
		if (obj instanceof Number || obj instanceof Boolean) {
			return obj.toString();
		}

		// Fallback
		return "\"" + escape(obj.toString()) + "\"";
	}

	private static String escape(String s) {
		return s.replace("\\", "\\\\").replace("\"", "\\\"");
	}
}
