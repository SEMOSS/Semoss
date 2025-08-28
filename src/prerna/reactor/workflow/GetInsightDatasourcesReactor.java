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
package prerna.reactor.workflow;

import java.util.List;
import java.util.Map;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetInsightDatasourcesReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		List<String> recipe = this.insight.getPixelList().getPixelRecipe();
		StringBuilder b = new StringBuilder();
		for (String s : recipe) {
			b.append(s);
		}
		String fullRecipe = b.toString();

		List<Map<String, Object>> sourcePixels = PixelUtility.getDatasourcesMetadata(this.insight.getUser(),
				fullRecipe);
		return new NounMetadata(sourcePixels, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}
}
