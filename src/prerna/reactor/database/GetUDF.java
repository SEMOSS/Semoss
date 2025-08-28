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
package prerna.reactor.database;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetUDF extends AbstractReactor {

	private static final String CLASS_NAME = GetUDF.class.getName();

	// takes in a the name and engine and mounts the database assets as that
	// variable name in both
	// python and R
	// I need to accomodate for when I should over ride
	// for instance a user could have saved a recipe with some mapping and then
	// later, they would like
	// to use a different mapping

	public GetUDF() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
		this.keyRequired = new int[]{1};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		// we may have the alias
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException(
					"Database " + databaseId + " does not exist or user does not have access to database");
		}

		IDatabaseEngine database = Utility.getDatabase(databaseId);

		if (database != null) {
			String[] output = database.getUDF();
			if (output != null) {
				return new NounMetadata(output, PixelDataType.VECTOR, PixelOperationType.OPERATION);
			} else {
				return getError("Database " + databaseId + " - Does not have any user defined functions ");
			}
		} else {
			return getError("No database " + databaseId + " - Please check your spelling / case");
		}
	}
}
