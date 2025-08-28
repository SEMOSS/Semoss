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
package prerna.reactor.model;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.modelinference.ModelInferenceQueryStruct;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.util.Utility;

public class ContextReactor extends AbstractQueryStructReactor {

	@Override
	protected AbstractQueryStruct createQueryStruct() {

		String context = null;
		if (!this.curRow.isEmpty()) {
			context = (String) this.curRow.get(0);
			if (context != null) {
				context = Utility.decodeURIComponent(context);
			}
		}

		((ModelInferenceQueryStruct) this.qs).setContext(context);
		return qs;
	}
}
