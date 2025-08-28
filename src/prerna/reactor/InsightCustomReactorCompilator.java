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

import java.util.HashSet;
import java.util.Set;
import prerna.om.Insight;

public class InsightCustomReactorCompilator {

	private static Set<String> compiled = new HashSet<>();

	private InsightCustomReactorCompilator() {
	}

	public static void setCompiled(String key) {
		compiled.add(key);
	}

	public static boolean isCompiled(String key) {
		return compiled.contains(key);
	}

	public static void reset(String key) {
		compiled.remove(key);
	}

	public static String getKey(Insight in) {
		if (in.isSavedInsight()) {
			return in.getProjectId() + "-" + in.getRdbmsId();
		}
		return in.getInsightId();
	}
}
