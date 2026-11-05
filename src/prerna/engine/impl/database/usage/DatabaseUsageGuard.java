/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.engine.impl.database.usage;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.impl.usage.EngineUsageGuard;
import prerna.engine.impl.usage.UsageLimiter;
import prerna.engine.impl.usage.UsageRule;
import prerna.om.ThreadStore;
import prerna.util.Constants;
import prerna.util.SystemDefaultEngines;

/**
 * Read adapter; the policy, cache, history and counters are catalog-neutral.
 */
public final class DatabaseUsageGuard {
	private DatabaseUsageGuard() {
	}

	public static boolean external(IDatabaseEngine engine) {
		String id = engine.getEngineId();
		return engine.getCatalogType() == IEngine.CATALOG_TYPE.DATABASE && id != null && !id.isBlank()
				&& !SystemDefaultEngines.getIgnoreDatabaseOwlList().contains(id)
				&& !id.endsWith(Constants.OWL_ENGINE_SUFFIX) && !id.endsWith(Constants.RDBMS_INSIGHTS_ENGINE_SUFFIX);
	}

	public static UsageLimiter.Ticket admit(IDatabaseEngine engine) {
		if (engine == null || !external(engine))
			return null;
		return EngineUsageGuard.admit(ThreadStore.getUser(), engine, UsageRule.Metric.QUERIES, 1);
	}
}
