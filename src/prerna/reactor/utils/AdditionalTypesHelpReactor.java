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
package prerna.reactor.utils;

import java.util.Map;
import prerna.algorithm.api.AdditionalDataType;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdditionalTypesHelpReactor extends AbstractReactor {

	/**
	 * This reactor allows the user to view the names of all additional types There
	 * are no inputs to the reactor
	 */
	private static String enumDescriptionsString = null;

	@Override
	public NounMetadata execute() {
		if (enumDescriptionsString == null) {
			Map<AdditionalDataType, String> mapOfEnumDescriptions = AdditionalDataType.getHelp();
			StringBuilder allDescriptions = new StringBuilder("Additional Types:\n");
			mapOfEnumDescriptions.forEach((adtlType, description) -> {
				allDescriptions.append("Name: ").append(adtlType).append(" | ").append("Description: ")
						.append(description).append(";\n");
			});

			enumDescriptionsString = allDescriptions.toString();
		}
		return new NounMetadata(enumDescriptionsString, PixelDataType.CONST_STRING, PixelOperationType.HELP);
	}
}
