/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.ds.r;

public class RregexValidator {

	public void Validate(String script) {
		// More can be added handled here once more incorrect R syntax is found.
		// If R script contains backslash, iterate over the user's input values,
		// if the value has a backslash, make sure the next value can be escaped.
		if (script.contains("\\")) {
			String[] inputs = script.split("\"");
			for (int i = 0; i < inputs.length; i++) {
				if (!((i & 1) == 0)) {
					String value = inputs[i];
					if (value.contains("\\")) {
						for (int j = 0; j < value.length(); j++) {
							char c = value.charAt(j);
							if (c == '\\') {
								if (!value.substring(j, j + 1).matches("\\|\'|\"|a|b|f|n|r|t|u|x|U")) {
									throw new IllegalArgumentException("Invalid Input!");
								}
							}
						}
					}
				}
			}
		}
	}
}
