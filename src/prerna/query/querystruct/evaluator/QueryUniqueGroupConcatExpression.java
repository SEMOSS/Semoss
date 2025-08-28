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
package prerna.query.querystruct.evaluator;

import java.util.LinkedHashSet;
import java.util.Set;

public class QueryUniqueGroupConcatExpression implements IQueryStructExpression {

	private Set<Object> objs = new LinkedHashSet<Object>();

	@Override
	public void processData(Object obj) {
		objs.add(obj);
	}

	@Override
	public Object getOutput() {
		StringBuilder concat = new StringBuilder();
		boolean first = true;
		for (Object obj : objs) {
			if (first) {
				concat.append(obj);
				first = false;
			} else {
				concat.append(", ").append(obj);
			}
		}
		return concat.toString();
	}
}
