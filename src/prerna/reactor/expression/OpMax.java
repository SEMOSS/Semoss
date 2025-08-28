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

public class OpMax extends OpBasicMath {

	public OpMax() {
		this.operation = "max";
	}

	@Override
	protected double evaluate(Object[] values) {
		return eval(values);
	}

	public static double eval(Object... values) {
		double max = -1.0 * Double.MAX_VALUE;

		for (Object val : values) {
			Double nextDouble = ((Number) val).doubleValue();
			max = max < nextDouble ? nextDouble : max;
		}
		return max;
	}

	public static double eval(double[] values) {
		double max = -1.0 * Double.MAX_VALUE;

		for (Object val : values) {
			Double nextDouble = ((Number) val).doubleValue();
			max = max < nextDouble ? nextDouble : max;
		}
		return max;
	}
}
