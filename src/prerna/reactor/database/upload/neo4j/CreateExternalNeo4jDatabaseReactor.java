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
package prerna.reactor.database.upload.neo4j;

import java.io.File;
import java.io.IOException;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.neo4j.Neo4jEngine;
import prerna.reactor.database.upload.gremlin.AbstractCreateExternalGraphReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadUtilities;

public class CreateExternalNeo4jDatabaseReactor extends AbstractCreateExternalGraphReactor {

	private String connectionStringKey;
	private String username;
	private String password;

	public CreateExternalNeo4jDatabaseReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONNECTION_STRING_KEY.getKey(),
				ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey(),
				ReactorKeysEnum.GRAPH_TYPE_ID.getKey(), ReactorKeysEnum.GRAPH_NAME_ID.getKey(),
				ReactorKeysEnum.GRAPH_METAMODEL.getKey(), ReactorKeysEnum.USE_LABEL.getKey()};
	}

	@Override
	protected void validateUserInput() throws IOException {
		this.connectionStringKey = this.keyValue.get(this.keysToGet[1]);
		if (this.connectionStringKey == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata(
					"Could not interpret a valid Connection URL (valid example: bolt://localhost:9999)",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		// Prepend jdbc keyword for neo4j
		// TODO jdbc::neo4j needs to be a constant
		connectionStringKey = "jdbc:neo4j:" + connectionStringKey;

		this.username = this.keyValue.get(this.keysToGet[2]);
		if (this.username == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Could not interpret username",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		this.password = this.keyValue.get(this.keysToGet[3]);
		if (this.password == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Could not interpret password",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
	}

	@Override
	protected File generateTempSmss(File owlFile) throws IOException {
		// the file path will become parameterized inside
		return UploadUtilities.generateTemporaryExternalNeo4jSmss(this.newDatabaseId, this.newDatabaseName, owlFile,
				this.connectionStringKey, this.username, this.password, this.typeMap, this.nameMap, useLabel());
	}

	@Override
	protected IDatabaseEngine generateEngine() throws Exception {
		Neo4jEngine neo4jDatabase = new Neo4jEngine();
		neo4jDatabase.setEngineId(this.newDatabaseId);
		neo4jDatabase.setEngineName(this.newDatabaseName);
		neo4jDatabase.open(this.smssFile.getAbsolutePath());
		return neo4jDatabase;
	}
}
