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

import java.util.Map;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class LoadInsightReactor extends OpenInsightReactor {

	public LoadInsightReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey(),
				ReactorKeysEnum.PARAM_KEY.getKey(), ReactorKeysEnum.ADDITIONAL_PIXELS.getKey(), CACHEABLE};
	}

	@Override
	public NounMetadata execute() {
		// i want to just run the open insight
		// and then i want to shift its insight id to be this insight
		// and update the insight store to replace to the one in the runner
		NounMetadata noun = super.execute();
		Map<String, Object> runnerMap = (Map<String, Object>) noun.getValue();
		PixelRunner runner = (PixelRunner) runnerMap.get("runner");

		Insight in = runner.getInsight();
		// remove the current insight id from the insight store + session store
		InsightStore.getInstance().remove(in.getInsightId());
		InsightStore.getInstance().removeFromSessionHash(getSessionId(), in.getInsightId());

		// reset the insight id and put in store
		in.setInsightId(this.insight.getInsightId());
		InsightStore.getInstance().put(in);

		// return the original noun from open insight
		noun.getOpType().clear();
		noun.addAdditionalOpTypes(PixelOperationType.LOAD_INSIGHT);
		return noun;
	}
}
