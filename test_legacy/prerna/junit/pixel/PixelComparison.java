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

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class PixelComparison {

	private final PixelJson expectedPixelJson;
	private final PixelJson actualPixelJson;
	private final Map<String, Object> differences;
	private final boolean different;
	
	public PixelComparison(PixelJson expectedPixelJson, PixelJson actualPixelJson, List<String> excludePaths, boolean ignoreOrder, PixelUnit runner) throws IOException {
		this.expectedPixelJson = expectedPixelJson;
		this.actualPixelJson = actualPixelJson;
		if (!expectedPixelJson.getPixelExpression().equals(actualPixelJson.getPixelExpression())) {
			throw new IllegalArgumentException("Unable to compare the results; the expected and actual pixel expressions are different.");
		}
		
		differences = runner.deepDiff(expectedPixelJson.getPixelOutput(), actualPixelJson.getPixelOutput(), excludePaths, ignoreOrder);
		different = differences.size() > 0;
	}
	
	public String getPixelExpression() {
		
		// Doesn't matter which one we pull from, since they are guaranteed to be the same
		return actualPixelJson.getPixelExpression();
	}
	
	public String getExpectedPixelOutput() {
		return expectedPixelJson.getPixelOutput();
	}
	
	public String getActualPixelOutput() {
		return actualPixelJson.getPixelOutput();
	}
	
	public Map<String, Object> getDifferences() {
		return differences;
	}
	
	public boolean isDifferent() {
		return different;
	}
	
	public boolean isDifferent(boolean ignoreAddedDictionary, boolean ignoreAddedIterable) {
		Map<String, Object> differences = this.differences;
		if (ignoreAddedDictionary) {
			differences.remove("dictionary_item_added");
		}
		if (ignoreAddedIterable) {
			differences.remove("iterable_item_added");
		}
		return differences.size() > 0;
	}
	
}
