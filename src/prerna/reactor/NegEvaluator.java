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
package prerna.reactor;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class NegEvaluator extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		NounMetadata noun = this.curRow.getNoun(0);
		return evalAdditiveInverseNoun(noun);
	}

	/**
	 * Return the additive inverse of a number
	 *
	 * @param noun
	 * @return
	 */
	private NounMetadata evalAdditiveInverseNoun(NounMetadata noun) {
		if (noun.getNounType() == PixelDataType.CONST_INT) {
			noun = new NounMetadata(-1 * ((Number) noun.getValue()).intValue(), PixelDataType.CONST_INT);
		} else if (noun.getNounType() == PixelDataType.CONST_DECIMAL) {
			noun = new NounMetadata(-1.0 * ((Number) noun.getValue()).doubleValue(), PixelDataType.CONST_DECIMAL);
		} else if (noun.getNounType() == PixelDataType.COLUMN) {
			String varName = noun.getValue().toString().trim();
			noun = planner.getVariableValue(varName);
			noun = evalAdditiveInverseNoun(noun);
		}
		// ugh.. you messed up at this point
		else {
			throw new IllegalArgumentException("Cannot take a negative of the value : " + noun.getValue());
		}

		return noun;
	}
}
