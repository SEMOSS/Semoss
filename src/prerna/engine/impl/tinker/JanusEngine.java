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
package prerna.engine.impl.tinker;

import java.util.Properties;
import org.janusgraph.core.JanusGraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.util.Utility;

public class JanusEngine extends TinkerEngine {

	private static final Logger classLogger = LoggerFactory.getLogger(JanusEngine.class);

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		String janusConfFilePath = SmssUtilities.getJanusFile(this.smssProp).getAbsolutePath();
		classLogger.info("Opening graph: " + Utility.cleanLogString(janusConfFilePath));
		g = JanusGraphFactory.open(janusConfFilePath);
		classLogger.info("Done opening graph: " + Utility.cleanLogString(janusConfFilePath));
	}

	@Override
	public DATABASE_TYPE getDatabaseType() {
		return IDatabaseEngine.DATABASE_TYPE.JANUS_GRAPH;
	}
}
