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
package prerna.reactor.insights;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class InsightHandleReactor extends AbstractReactor {

	/**
	 * This is just an echo back for the default handle that is backing this insight
	 */
	@Override
	public NounMetadata execute() {
		String encodedValue = this.curRow.get(0).toString();
		String decodedText = Utility.decodeURIComponent(encodedValue);
		return new NounMetadata(decodedText, PixelDataType.CONST_STRING, PixelOperationType.INSIGHT_HANDLE);
	}
}
