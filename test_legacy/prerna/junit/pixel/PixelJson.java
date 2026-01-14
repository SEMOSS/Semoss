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
package prerna.junit.pixel;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import prerna.util.gson.GsonUtility;

public class PixelJson {
	
	private final JsonObject json;
	private final String pixelExpression;
	private final String pixelOutput;
	
	private static final Gson GSON_PRETTY = GsonUtility.getDefaultGson(true);
	
	private static final String LS = System.getProperty("line.separator");

	public PixelJson(JsonObject json) {
		this.json = json;
		
		if (!(json.has("pixelExpression") && json.has("output"))) {
			String message = LS
					+ "Each expected JSON for this test must contain both a \"pixelExpression\" and its corresponding \"output\" in the form: " + LS
					+ LS
					+ "{" + LS
					+ "   \"pixelExpression\":\"<pixel>;\"," + LS
					+ "   \"output\":{" + LS
					+ "      <output json>"+ LS
					+ "   }(,... <any additional members, which are ignored>)" + LS
					+ "}" + LS
					+ LS
					+ "Or (when compare_all = true): " + LS
					+ LS
					+ "[" + LS
					+ "   {" + LS
					+ "      \"pixelExpression\":\"<pixel>;\"," + LS
					+ "      \"output\":{" + LS
					+ "         <output json>"+ LS
					+ "      }(,... <any additional members, which are ignored>)" + LS
					+ "   }(,... <additional pixel outputs from the recipe>)" + LS
					+ "]";
			
			throw new IllegalArgumentException(message);
		}
		
		this.pixelExpression = json.get("pixelExpression").getAsString();
		this.pixelOutput = GSON_PRETTY.toJson(json.get("output"));
	}

	public JsonObject getJson() {
		return json;
	}

	public String getPixelOutput() {
		return pixelOutput;
	}

	public String getPixelExpression() {
		return pixelExpression;
	}
	
}
