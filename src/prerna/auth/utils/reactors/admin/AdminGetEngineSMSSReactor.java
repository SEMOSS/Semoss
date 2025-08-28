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
package prerna.auth.utils.reactors.admin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AdminGetEngineSMSSReactor extends AbstractReactor {

	public AdminGetEngineSMSSReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (engineId == null) {
			throw new IllegalArgumentException("Need to define the engine");
		}

		IEngine engine = Utility.getEngine(engineId);
		String currentSmssFileLocation = engine.getSmssFilePath();
		Path currentSmssPath = Paths.get(currentSmssFileLocation);

		if (!Files.exists(currentSmssPath) || !Files.isRegularFile(currentSmssPath)) {
			throw new IllegalArgumentException("Could not find smss file for engine " + engineId
					+ ". Please reach out to an administrator for assistance");
		}

		String currentSmssContent = null;
		try {
			currentSmssContent = new String(Files.readAllBytes(Paths.get(currentSmssPath.toUri())));
		} catch (IOException e) {
			throw new IllegalArgumentException(
					"An error occurred reading the current engine smss details. Detailed message = " + e.getMessage());
		}

		String concealedSmssContent = SmssUtilities.concealSmssSensitiveInfo(currentSmssContent);
		return new NounMetadata(concealedSmssContent, PixelDataType.CONST_STRING);
	}
}
