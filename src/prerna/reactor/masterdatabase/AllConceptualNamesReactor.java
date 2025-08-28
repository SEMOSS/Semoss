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
package prerna.reactor.masterdatabase;

import java.util.Collection;
import java.util.List;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AllConceptualNamesReactor extends AbstractReactor {

	/** Return all the conceptual names */
	@Override
	public NounMetadata execute() {
		// need to take into consideration security
		List<String> engineFilters = SecurityEngineUtils.getFullUserEngineIds(this.insight.getUser());
		Collection<String> conceptualNames = MasterDatabaseUtility.getAllConceptualNames(engineFilters);
		return new NounMetadata(conceptualNames, PixelDataType.CONST_STRING);
	}
}
