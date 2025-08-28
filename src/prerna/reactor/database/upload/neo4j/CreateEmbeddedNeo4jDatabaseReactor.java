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
// package prerna.sablecc2.reactor.database.upload.neo4j;
//
// import java.io.File;
// import java.io.IOException;
//
// import prerna.engine.api.IDatabaseEngine;
// import prerna.engine.impl.neo4j.Neo4jEmbeddedEngine;
// import prerna.sablecc2.om.ReactorKeysEnum;
// import prerna.sablecc2.reactor.database.upload.gremlin.AbstractCreateExternalGraphReactor;
// import prerna.util.upload.UploadInputUtility;
// import prerna.util.upload.UploadUtilities;
//
/// *
// * Since neo4j-tinkerpop-api-impl is no longer supported
// * Removing logic around interacting with neo4j through gremlin
// */
//
// public class CreateEmbeddedNeo4jDatabaseReactor extends AbstractCreateExternalGraphReactor {
//
//	private String filePath;
//
//	public CreateEmbeddedNeo4jDatabaseReactor() {
//		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(),
// ReactorKeysEnum.FILE_PATH.getKey(),
//				ReactorKeysEnum.GRAPH_TYPE_ID.getKey(), ReactorKeysEnum.GRAPH_NAME_ID.getKey(),
//				ReactorKeysEnum.GRAPH_METAMODEL.getKey(), ReactorKeysEnum.USE_LABEL.getKey(),
//				ReactorKeysEnum.SPACE.getKey() };
//	}
//
//	@Override
//	protected void validateUserInput() throws IOException {
//		this.filePath = UploadInputUtility.getFilePath(this.store, this.insight);
//	}
//
//	@Override
//	protected File generateTempSmss(File owlFile) throws IOException {
//		return UploadUtilities.generateTemporaryEmbeddedNeo4jSmss(
//				this.newDatabaseId, this.newDatabaseName, owlFile, this.filePath, this.typeMap, this.nameMap,
// useLabel());	}
//
//	@Override
//	protected IDatabaseEngine generateEngine() throws Exception {
//		Neo4jEmbeddedEngine neo4jDatabase = new Neo4jEmbeddedEngine();
//		neo4jDatabase.setEngineId(this.newDatabaseId);
//		neo4jDatabase.setEngineName(this.newDatabaseName);
//		neo4jDatabase.open(this.smssFile.getAbsolutePath());
//		return neo4jDatabase;
//	}
//	}
