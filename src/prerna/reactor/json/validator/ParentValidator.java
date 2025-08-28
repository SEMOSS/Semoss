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
package prerna.reactor.json.validator;

import java.util.Hashtable;
import prerna.reactor.json.GreedyJsonReactor;

public class ParentValidator extends GreedyJsonReactor {

	// the method to implement here is validate
	// In our case the super parent will not throw any error
	public void process() {

		// shallow validation
		// data validation
		// business rule validation

		Hashtable<String, Object> allInputs = this.store.getDataHash();
		for (String key : allInputs.keySet()) {
			System.out.println("key = " + key + " , value = " + allInputs.get(key));
		}

		// // I will throw a random error here
		// addError("ABC", "Child should behave like one.. right now it is not");
		// // also add a stage here
		// addErrorWithStage("DEF", "Child should behave like one.. right now it is
		// not",
		// "BRE_VALIDATION");
	}
}
