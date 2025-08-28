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
package prerna.ds.rdbms.h2;

public class H2MedianAggregation implements org.h2.api.AggregateFunction {

	java.util.LinkedList<Double> values = new java.util.LinkedList<Double>();

	@Override
	public void init(java.sql.Connection cnctn) throws java.sql.SQLException {
		// what do i do for this????
	}

	@Override
	public int getType(int[] ints) throws java.sql.SQLException {
		return java.sql.Types.DOUBLE;
	}

	@Override
	public void add(Object o) throws java.sql.SQLException {
		if (o != null) {
			this.values.add(((Number) o).doubleValue());
		}
	}

	@Override
	public Object getResult() throws java.sql.SQLException {
		// Sort list
		java.util.Collections.sort(this.values);

		// Return median
		int size = this.values.size();
		if (size > 0) {
			int pos = ((int) size / 2);
			// Odd size
			if ((size % 2) == 1)
				return this.values.get(pos);
			// Even size
			else
				return new Double((this.values.get(pos - 1) + this.values.get(pos)) / 2);
		} else {
			return null;
		}
	}
}
