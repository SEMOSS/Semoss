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

import java.sql.SQLException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.utils.SecurityShareSessionUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GetShareSessionTokenReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetShareSessionTokenReactor.class);

	@Override
	public NounMetadata execute() {
		String token;
		try {
			token = SecurityShareSessionUtils.createShareToken(this.insight.getUser(), getSessionId(), getRouteId());
		} catch (SQLException e) {
			classLogger.error(e.getMessage());
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not create share token.");
		}
		NounMetadata noun = new NounMetadata(token, PixelDataType.CONST_STRING);
		return noun;
	}
}
