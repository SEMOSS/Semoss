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
package prerna.reactor.expression;

import java.util.Arrays;

public class OpSmall extends OpBasicMath {

	public OpSmall() {
		this.operation = "small";
	}

	@Override
	protected double evaluate(Object[] values) {
		return eval(values);
	}

	public static double eval(Object... values) {
		// grab the index
		int valIndex = ((Number) values[values.length - 1]).intValue();
		// convert everything to a double array except the last index
		double[] doubleValues = convertToDoubleArray(values, 0, values.length - 1);
		// sort in ascending order
		Arrays.sort(doubleValues);
		// return the index - 1
		// remember, excel is 1 based
		return doubleValues[valIndex - 1];
	}

	public static double eval(double[] values) {
		// grab the index
		int valIndex = ((Number) values[values.length - 1]).intValue();
		// convert everything to a double array except the last index
		double[] doubleValues = Arrays.copyOf(values, values.length - 1);
		// sort in ascending order
		Arrays.sort(doubleValues);
		// return the index - 1
		// remember, excel is 1 based
		return doubleValues[valIndex - 1];
	}
}
