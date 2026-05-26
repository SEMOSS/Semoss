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
package prerna.engine.impl.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Vector;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.pgvector.PGvector;

import prerna.SemossUnitTest;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.date.SemossDate;
import prerna.ds.py.PyTranslator;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.vector.metadata.VectorDatabaseMetadataCSVTable;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.security.HttpHelperUtility;
import prerna.tcp.client.SocketClient;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.QueryExecutionUtility;
import prerna.util.SymlinkHelper;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.H2QueryUtil;
import prerna.util.sql.SqlQueryUtilFactory;

public class PGVectorDatabaseEngineUnitTests extends SemossUnitTest {
	private User user;
	private Insight insight;
	private PGVectorDatabaseEngine engine;
	private IModelEngine modelEmbedder;

	@BeforeEach
	void setUp() throws IOException {
		FileUtils.cleanDirectory(tempDir.toFile());

		user = mock(User.class);
		engine = new PGVectorDatabaseEngine();
		insight = mock(Insight.class);
		modelEmbedder = mock(IModelEngine.class);
	}

	@Test
	void testOpen() throws Exception {
		Properties testProps = new Properties();
		String connPooling = "false";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ENGINE_ALIAS";
		String testRDBMSType = "H2_DB";
		String url = "http://fake.url/";
		String testVectorTableName = "TEST_TABLE_NAME";
		String testKeepInputOutput = "false";
		String databaseZone = "America/Chicago";
		String owl = "REMAKE";
		testProps.setProperty(Constants.USE_CONNECTION_POOLING, connPooling);
		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.RDBMS_TYPE, testRDBMSType);
		testProps.setProperty("PGVECTOR_TABLE_NAME", testVectorTableName);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, testKeepInputOutput);
		testProps.setProperty(Constants.DATABASE_ZONEID, databaseZone);
		testProps.setProperty(Constants.OWL, owl);

		String engineNameAndId = SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(engineNameAndId);
		Path engineAssetFolder = engineFolder.resolve("assets");
		Path engineVersionFolder = engineFolder.resolve("version");

		try (MockedStatic<SmssUtilities> su = Mockito.mockStatic(SmssUtilities.class);
				MockedStatic<SqlQueryUtilFactory> squf = Mockito.mockStatic(SqlQueryUtilFactory.class);
				MockedStatic<AbstractSqlQueryUtil> asqu = Mockito.mockStatic(AbstractSqlQueryUtil.class);
				MockedStatic<PGvector> pgv = Mockito.mockStatic(PGvector.class);) {
			// used in AbstractDatabaseEngine.open();
			su.when(() -> SmssUtilities.getDataFile(any())).thenReturn(null);
			su.when(() -> SmssUtilities.getUniqueName(any(), any())).thenCallRealMethod();
			// used in RDBMSNativeEngine.open()
			H2QueryUtil queryUtilMock = mock();
			{
				when(queryUtilMock.getConnectionUserKey()).thenReturn("user_key");
				when(queryUtilMock.getConnectionPasswordKey()).thenReturn("password_key");
				when(queryUtilMock.setConnectionDetailsFromSMSS(any())).thenReturn(null);
			}
			squf.when(() -> SqlQueryUtilFactory.initialize(any())).thenReturn(queryUtilMock);
			Connection mockConn = mock();
			asqu.when(() -> AbstractSqlQueryUtil.makeConnection(any(AbstractSqlQueryUtil.class), any(String.class),
					any(CaseInsensitiveProperties.class))).thenReturn(mockConn);
			// used in PGVectorDatabaseEngine.open()
			pgv.when(() -> PGvector.addVectorType(mockConn)).then(invocationOnMock -> null);
			Statement mockStm = mock(Statement.class);// RDBMSNativeEngine.open() -> used in initSQL() ->
														// execCreateStatement()
			when(mockStm.execute(any(String.class))).thenReturn(true);// RDBMSNativeEngine.open() -> used in initSQL()
																		// -> execCreateStatement()
			when(mockConn.createStatement()).thenReturn(mockStm);// RDBMSNativeEngine.open() -> used in initSQL() ->
																	// execCreateStatement()

			try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
				DIHelper diMock = mock(DIHelper.class);
				dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
				when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
				try (MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);
						MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class)) {

					eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR,
							engineNameAndId)).thenReturn(engineFolder.toString());
					eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR,
							engineNameAndId)).thenReturn(engineAssetFolder.toString());
					eu.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.VECTOR,
							engineNameAndId)).thenReturn(engineVersionFolder.toString());
					eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
							testEngineAlias)).thenReturn(engineAssetFolder.toString());

					engine.open(testProps);
					assertTrue(Files.exists(engineAssetFolder));
					Properties engineProperties = engine.getSmssProp();
					assertNotNull(engineProperties);
					assertFalse(engineProperties.isEmpty());
					assertFalse(testProps.isEmpty());
					for (Entry<Object, Object> testProp : testProps.entrySet()) {
						assertTrue(engineProperties.containsKey(testProp.getKey()));
						assertTrue(engineProperties.containsValue(testProp.getValue()));
					}
					assertTrue(engineProperties.containsKey(Constants.CONNECTION_URL));
					assertTrue(engineProperties.containsValue(url));
				}
			}
		}
	}

	@Test
	void testOpenNoVectorTableName() throws Exception {
		Properties testProps = new Properties();
		String connPooling = "false";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ENGINE_ALIAS";
		String testRDBMSType = "H2_DB";
		String url = "http://fake.url/";
		String testVectorTableName = "TEST_TABLE_NAME";
		String testKeepInputOutput = "false";
		String databaseZone = "America/Chicago";
		String owl = "REMAKE";
		testProps.setProperty(Constants.USE_CONNECTION_POOLING, connPooling);
		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.RDBMS_TYPE, testRDBMSType);
//		testProps.setProperty("PGVECTOR_TABLE_NAME", testVectorTableName);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, testKeepInputOutput);
		testProps.setProperty(Constants.DATABASE_ZONEID, databaseZone);
		testProps.setProperty(Constants.OWL, owl);

		String engineNameAndId = SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(engineNameAndId);
		Path engineAssetFolder = engineFolder.resolve("assets");
		Path engineVersionFolder = engineFolder.resolve("version");

		try (MockedStatic<SmssUtilities> su = Mockito.mockStatic(SmssUtilities.class);
				MockedStatic<SqlQueryUtilFactory> squf = Mockito.mockStatic(SqlQueryUtilFactory.class);
				MockedStatic<AbstractSqlQueryUtil> asqu = Mockito.mockStatic(AbstractSqlQueryUtil.class);
				MockedStatic<PGvector> pgv = Mockito.mockStatic(PGvector.class);) {
			// used in AbstractDatabaseEngine.open();
			su.when(() -> SmssUtilities.getDataFile(any())).thenReturn(null);
			su.when(() -> SmssUtilities.getUniqueName(any(), any())).thenCallRealMethod();
			// used in RDBMSNativeEngine.open()
			H2QueryUtil queryUtilMock = mock();
			{
				when(queryUtilMock.getConnectionUserKey()).thenReturn("user_key");
				when(queryUtilMock.getConnectionPasswordKey()).thenReturn("password_key");
				when(queryUtilMock.setConnectionDetailsFromSMSS(any())).thenReturn(null);
			}
			squf.when(() -> SqlQueryUtilFactory.initialize(any())).thenReturn(queryUtilMock);
			Connection mockConn = mock();
			asqu.when(() -> AbstractSqlQueryUtil.makeConnection(any(AbstractSqlQueryUtil.class), any(String.class),
					any(CaseInsensitiveProperties.class))).thenReturn(mockConn);
			// used in PGVectorDatabaseEngine.open()
			pgv.when(() -> PGvector.addVectorType(mockConn)).then(invocationOnMock -> null);
			Statement mockStm = mock(Statement.class);// RDBMSNativeEngine.open() -> used in initSQL() ->
														// execCreateStatement()
			when(mockStm.execute(any(String.class))).thenReturn(true);// RDBMSNativeEngine.open() -> used in initSQL()
																		// -> execCreateStatement()
			when(mockConn.createStatement()).thenReturn(mockStm);// RDBMSNativeEngine.open() -> used in initSQL() ->
																	// execCreateStatement()

			try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
				DIHelper diMock = mock(DIHelper.class);
				dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
				when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
				try (MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);
						MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);) {

					eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR,
							engineNameAndId)).thenReturn(engineFolder.toString());
					eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR,
							engineNameAndId)).thenReturn(engineAssetFolder.toString());
					eu.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.VECTOR,
							engineNameAndId)).thenReturn(engineVersionFolder.toString());
					eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
							testEngineAlias)).thenReturn(engineAssetFolder.toString());

					NullPointerException e = assertThrows(NullPointerException.class, () -> engine.open(testProps));
					assertEquals("Must define the vector db table name", e.getMessage());
				}
			}
		}
	}

	@Test
	void testOpenIncorrectChunkUnit() throws Exception {
		Properties testProps = new Properties();
		String connPooling = "false";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ENGINE_ALIAS";
		String testRDBMSType = "H2_DB";
		String url = "http://fake.url/";
		String testVectorTableName = "TEST_TABLE_NAME";
		String testKeepInputOutput = "false";
		String databaseZone = "America/Chicago";
		String owl = "REMAKE";
		testProps.setProperty(Constants.USE_CONNECTION_POOLING, connPooling);
		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.RDBMS_TYPE, testRDBMSType);
		testProps.setProperty("PGVECTOR_TABLE_NAME", testVectorTableName);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, testKeepInputOutput);
		testProps.setProperty(Constants.DATABASE_ZONEID, databaseZone);
		testProps.setProperty(Constants.OWL, owl);
		testProps.setProperty(Constants.DEFAULT_CHUNK_UNIT, "bad_unit");

		String engineNameAndId = SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(engineNameAndId);
		Path engineAssetFolder = engineFolder.resolve("assets");
		Path engineVersionFolder = engineFolder.resolve("version");

		try (MockedStatic<SmssUtilities> su = Mockito.mockStatic(SmssUtilities.class);
				MockedStatic<SqlQueryUtilFactory> squf = Mockito.mockStatic(SqlQueryUtilFactory.class);
				MockedStatic<AbstractSqlQueryUtil> asqu = Mockito.mockStatic(AbstractSqlQueryUtil.class);
				MockedStatic<PGvector> pgv = Mockito.mockStatic(PGvector.class);) {
			// used in AbstractDatabaseEngine.open();
			su.when(() -> SmssUtilities.getDataFile(any())).thenReturn(null);
			su.when(() -> SmssUtilities.getUniqueName(any(), any())).thenCallRealMethod();
			// used in RDBMSNativeEngine.open()
			H2QueryUtil queryUtilMock = mock();
			{
				when(queryUtilMock.getConnectionUserKey()).thenReturn("user_key");
				when(queryUtilMock.getConnectionPasswordKey()).thenReturn("password_key");
				when(queryUtilMock.setConnectionDetailsFromSMSS(any())).thenReturn(null);
			}
			squf.when(() -> SqlQueryUtilFactory.initialize(any())).thenReturn(queryUtilMock);
			Connection mockConn = mock();
			asqu.when(() -> AbstractSqlQueryUtil.makeConnection(any(AbstractSqlQueryUtil.class), any(String.class),
					any(CaseInsensitiveProperties.class))).thenReturn(mockConn);
			// used in PGVectorDatabaseEngine.open()
			pgv.when(() -> PGvector.addVectorType(mockConn)).then(invocationOnMock -> null);
			Statement mockStm = mock(Statement.class);// RDBMSNativeEngine.open() -> used in initSQL() ->
														// execCreateStatement()
			when(mockStm.execute(any(String.class))).thenReturn(true);// RDBMSNativeEngine.open() -> used in initSQL()
																		// -> execCreateStatement()
			when(mockConn.createStatement()).thenReturn(mockStm);// RDBMSNativeEngine.open() -> used in initSQL() ->
																	// execCreateStatement()

			try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
				DIHelper diMock = mock(DIHelper.class);
				dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
				when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
				try (MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);
						MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);) {

					eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR,
							engineNameAndId)).thenReturn(engineFolder.toString());
					eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR,
							engineNameAndId)).thenReturn(engineAssetFolder.toString());
					eu.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.VECTOR,
							engineNameAndId)).thenReturn(engineVersionFolder.toString());
					eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
							testEngineAlias)).thenReturn(engineAssetFolder.toString());

					IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
							() -> engine.open(testProps));
					assertEquals("DEFAULT_CHUNK_UNIT should be either 'tokens' or 'characters'", e.getMessage());
				}
			}
		}
	}

	@Test
	void testAddEmbeddings() throws Exception {
		String testVectorTableName = "TEST_TABLE_NAME"; // set in openEngine() method
		String createVectorTableQuery = createTestVectorTableString(testVectorTableName);
		Map<String, Object> parameters = new HashMap<>();
		List<List<Double>> allEmbeddings = new Vector<>();
		List<Double> firstRowEmbeddings = new Vector<>();
		{
			firstRowEmbeddings.add(0.2);
			firstRowEmbeddings.add(0.4);
			firstRowEmbeddings.add(0.6);
			firstRowEmbeddings.add(0.8);
			allEmbeddings.add(firstRowEmbeddings);
		}

		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties, connection

		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		verifyModelProps(engine, testEmbedderId, embedderModel, embedderModelType);

		// Create a temporary database file
		Path dbFile = tempDir.resolve("test_database.db");
		String dbUrl = "jdbc:sqlite:" + dbFile.toString();

		// Set up the database
		try (Connection connection = DriverManager.getConnection(dbUrl);
				MockedStatic<AbstractSqlQueryUtil> asqu = Mockito.mockStatic(AbstractSqlQueryUtil.class);
				MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);) {

			try (Statement statement = connection.createStatement()) {
				statement.execute(createVectorTableQuery);
			}

			String source = "testSource";
			String modality = "testModality";
			String divider = "testDivider";
			String part = "testPart";
			Number tokens = 10;
			String content = "testContent";

			VectorDatabaseCSVTable dataTable = new VectorDatabaseCSVTable();
			dataTable.addRow(source, modality, divider, part, tokens, content);

			asqu.when(() -> AbstractSqlQueryUtil.makeConnection(any(AbstractSqlQueryUtil.class), any(String.class),
					any(CaseInsensitiveProperties.class))).thenReturn(connection);

			u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			// used in VectorDatabaseCSVTable.generateAndAssingEmbeddings()
			EmbeddingsModelEngineResponse outputMock = mock(EmbeddingsModelEngineResponse.class);
			when(modelEmbedder.embeddings(any(), any(), nullable(Map.class))).thenReturn(outputMock);
			when(outputMock.getResponse()).thenReturn(allEmbeddings);

			engine.addEmbeddings(dataTable, insight, parameters);

			// Verify data
			try (Statement statement = connection.createStatement()) {
				ResultSet resultSet = statement.executeQuery("SELECT * from " + testVectorTableName);
				int resultSetCount = 0;
				if (resultSet.next()) {
					resultSetCount++;
					String actualEmbeddingsStr = (String) resultSet.getObject("EMBEDDING");
					String embeddingsString = "["
							+ String.join(",",
									firstRowEmbeddings.stream().map(String::valueOf).collect(Collectors.toList()))
							+ "]";
					assertEquals(embeddingsString, actualEmbeddingsStr);
					assertEquals(source, resultSet.getString("SOURCE"));
					assertEquals(modality, resultSet.getString("MODALITY"));
					assertEquals(divider, resultSet.getString("DIVIDER"));
					assertEquals(part, resultSet.getString("PART"));
					assertEquals(tokens, resultSet.getInt("TOKENS"));
					assertEquals(content, resultSet.getString("CONTENT"));
				}
				assertEquals(1, resultSetCount);
			}
		}
	}

	private String createTestVectorTableString(String tableName) {
		String query = "CREATE TABLE " + tableName + " (EMBEDDING  blob, " + "SOURCE varchar(100), "
				+ "MODALITY varchar(100), " + "DIVIDER varchar(100), " + "PART varchar(100), " + "TOKENS int, "
				+ "CONTENT varchar(100))";
		return query;
	}

	@Test
	void testAddEmbeddingsNoInsight() throws Exception {
		Map<String, Object> parameters = new HashMap<>();
		VectorDatabaseCSVTable vectorCsvTable = new VectorDatabaseCSVTable();

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> engine.addEmbeddings(vectorCsvTable, null, parameters));
		assertEquals("Insight must be provided to run Model Engine Encoder", e.getMessage());
	}

	@Test
	void testAddEmbedding() throws Exception {
		String testVectorTableName = "TEST_TABLE_NAME"; // set in openEngine() method
		String createVectorTableQuery = createTestVectorTableString(testVectorTableName);
		Map<String, Object> parameters = new HashMap<>();
		List<Double> embeddings = new Vector<>();
		{
			embeddings.add(0.2);
			embeddings.add(0.4);
			embeddings.add(0.6);
			embeddings.add(0.8);
		}

		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties, connection

		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		verifyModelProps(engine, testEmbedderId, embedderModel, embedderModelType);

		// Create a temporary database file
		Path dbFile = tempDir.resolve("test_database.db");
		String dbUrl = "jdbc:sqlite:" + dbFile.toString();

		// Set up the database
		try (Connection connection = DriverManager.getConnection(dbUrl);
				MockedStatic<AbstractSqlQueryUtil> asqu = Mockito.mockStatic(AbstractSqlQueryUtil.class);) {

			try (Statement statement = connection.createStatement()) {
				statement.execute(createVectorTableQuery);
			}
			String source = "testSource";
			String modality = "testModality";
			String divider = "testDivider";
			String part = "testPart";
			int tokens = 10;
			String content = "testContent";

			asqu.when(() -> AbstractSqlQueryUtil.makeConnection(any(AbstractSqlQueryUtil.class), any(String.class),
					any(CaseInsensitiveProperties.class))).thenReturn(connection);

			engine.addEmbedding(embeddings, source, modality, divider, part, tokens, content, null);

			// Verify data
			try (Statement statement = connection.createStatement()) {
				ResultSet resultSet = statement.executeQuery("SELECT * from " + testVectorTableName);
				int resultSetCount = 0;
				if (resultSet.next()) {
					resultSetCount++;
					String actualEmbeddingsStr = (String) resultSet.getObject("EMBEDDING");
					String embeddingsString = "["
							+ String.join(",", embeddings.stream().map(String::valueOf).collect(Collectors.toList()))
							+ "]";
					assertEquals(embeddingsString, actualEmbeddingsStr);
					assertEquals(source, resultSet.getString("SOURCE"));
					assertEquals(modality, resultSet.getString("MODALITY"));
					assertEquals(divider, resultSet.getString("DIVIDER"));
					assertEquals(part, resultSet.getString("PART"));
					assertEquals(tokens, resultSet.getInt("TOKENS"));
					assertEquals(content, resultSet.getString("CONTENT"));
				}
				assertEquals(1, resultSetCount);
			}
		}
	}

	@Test
	void testAddEmbeddingExecuteFailed() throws Exception {
		List<Double> embedding = new Vector<>();
		String source = "source";
		String modality = "modality";
		String divider = "divider";
		String part = "part";
		int tokens = 4;
		String content = "content";
		Map<String, Object> additionalMetadata = new HashMap<>();

		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties, connection

		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		verifyModelProps(engine, testEmbedderId, embedderModel, embedderModelType);

		PreparedStatement psMock = mock(PreparedStatement.class);
		try (MockedStatic<AbstractSqlQueryUtil> asqu = Mockito.mockStatic(AbstractSqlQueryUtil.class);) {
			Connection mockConn = mock(Connection.class);
			asqu.when(() -> AbstractSqlQueryUtil.makeConnection(any(AbstractSqlQueryUtil.class), any(String.class),
					any(CaseInsensitiveProperties.class))).thenReturn(mockConn);

			// used in queryUtil.enhanceConnection()
			Statement mockStm = mock(Statement.class);
			when(mockStm.execute(any(String.class))).thenReturn(true);
			when(mockConn.createStatement()).thenReturn(mockStm);
			doNothing().when(mockConn).close();
			// used in addEmbedding()
			when(mockConn.prepareStatement(any())).thenReturn(psMock);
			doNothing().when(psMock).setObject(any(int.class), any(Object.class));
			doNothing().when(psMock).setString(any(int.class), any(String.class));
			doNothing().when(psMock).setInt(any(int.class), any(int.class));
			when(psMock.executeUpdate()).thenReturn(PreparedStatement.EXECUTE_FAILED);
			when(mockConn.getAutoCommit()).thenReturn(true);

			SQLException e = assertThrows(SQLException.class, () -> engine.addEmbedding(embedding, source, modality,
					divider, part, tokens, content, additionalMetadata));
			assertEquals("Error inserting embeddings data", e.getMessage());
		}
	}

	@Test
	void testRemoveDocument() throws Exception {
		String indexClass = "index_class";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ENGINE_ALIAS";
		openEngine(tempDir, engine, null); // set initial properties

		Map<String, Object> parameters = new HashMap<>();
		parameters.put("indexClass", indexClass);

		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER)
				.resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path indexDirPath = Paths.get(engineFolder.toString(), "assets", "schema", indexClass);
		Files.createDirectories(indexDirPath);
		// create schema/index_class/documents
		Path docDirPath = indexDirPath.resolve(AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME);
		Files.createDirectories(docDirPath);
		// create 4 new files: newFile1 ... newFile4.txt
		List<String> fileNames = new Vector<>();
		for (int fileNum = 1; fileNum < 5; fileNum++) {
			String fileName = "newFile" + fileNum + ".txt";
			fileNames.add(fileName);
			Path newFilePath = docDirPath.resolve(fileName);
			Files.createFile(newFilePath);
		}

		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
				MockedStatic<AbstractSqlQueryUtil> asqu = Mockito.mockStatic(AbstractSqlQueryUtil.class);) {

			// used in getConnection()
			Connection mockConn = mock(Connection.class);
			asqu.when(() -> AbstractSqlQueryUtil.makeConnection(any(AbstractSqlQueryUtil.class), any(String.class),
					any(CaseInsensitiveProperties.class))).thenReturn(mockConn);
			PreparedStatement psMock = mock();
			when(mockConn.prepareStatement(any())).thenReturn(psMock);
			when(psMock.executeBatch()).thenReturn(new int[0]);

			fileNames.forEach(
					fileName -> assertTrue(Files.exists(docDirPath.resolve(fileName)), fileName + " should exist"));
			engine.removeDocument(fileNames, parameters);
			fileNames.forEach(fileName -> assertFalse(Files.exists(docDirPath.resolve(fileName)),
					fileName + " should not exist"));

		}
	}

	@Test
	void testAddMetadata() throws Exception {
		String testMetadataTableName = "TEST_TABLE_NAME_METADATA"; // set in openEngine() method
		String createTableQuery = createTestMetadataTableString(testMetadataTableName);

		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties, connection

		// Create a temporary database file
		Path dbFile = tempDir.resolve("test_database.db");
		String dbUrl = "jdbc:sqlite:" + dbFile.toString();

		// Set up the database
		try (Connection connection = DriverManager.getConnection(dbUrl);
				MockedStatic<AbstractSqlQueryUtil> asqu = Mockito.mockStatic(AbstractSqlQueryUtil.class);) {

			try (Statement statement = connection.createStatement()) {
				statement.execute(createTableQuery);
			}

			String source = "testSource";
			String attribute = "testAtribute";
			String strValue = "strValue";
			Number intValue = 10;
			Number numValue = 5.555;
			Boolean boolValue = true;
			Date currDate = new Date();
			SemossDate dateValue = new SemossDate(currDate);
			SemossDate timestampValue = new SemossDate(currDate);

			VectorDatabaseMetadataCSVTable metadataTable = new VectorDatabaseMetadataCSVTable();
			metadataTable.addRow(source, attribute, strValue, intValue, numValue, boolValue, dateValue, timestampValue);

			asqu.when(() -> AbstractSqlQueryUtil.makeConnection(any(AbstractSqlQueryUtil.class), any(String.class),
					any(CaseInsensitiveProperties.class))).thenReturn(connection);

			engine.addMetadata(metadataTable);

			// Verify data
			try (Statement statement = connection.createStatement()) {
				ResultSet resultSet = statement.executeQuery("SELECT * from " + testMetadataTableName);
				int resultSetCount = 0;
				if (resultSet.next()) {
					resultSetCount++;
					assertEquals(source, resultSet.getString("SOURCE"));
					assertEquals(attribute, resultSet.getString("ATTRIBUTE"));
					assertEquals(strValue, resultSet.getString("STR_VALUE"));
					assertEquals(intValue, resultSet.getInt("INT_VALUE"));
					assertEquals(numValue, resultSet.getDouble("NUM_VALUE"));
					assertEquals(boolValue, resultSet.getBoolean("BOOL_VALUE"));
					Date dateFromLocalDT = Date.from(LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant());
					assertEquals(dateFromLocalDT, resultSet.getDate("DATE_VAL"));
					assertEquals(currDate, resultSet.getTimestamp("TIMESTAMP_VAL"));
				}
				assertEquals(1, resultSetCount);
			}
		}
	}

	private String createTestMetadataTableString(String tableName) {
		String query = "CREATE TABLE " + tableName + " (SOURCE varchar(100), " + "ATTRIBUTE varchar(100), "
				+ "STR_VALUE varchar(100), " + "INT_VALUE int, " + "NUM_VALUE decimal(20,4), " + "BOOL_VALUE bit, "
				+ "DATE_VAL Date, " + "TIMESTAMP_VAL datetime)";
		return query;
	}

	@Test
	void testNearestNeighborCall() throws Exception {
		Number limit = 1;
		String searchStatement = "searchStatement";
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties, connection

		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		verifyModelProps(engine, testEmbedderId, embedderModel, embedderModelType);

		List<List<Double>> allEmbeddings = new Vector<>();
		List<Double> firstRowEmbeddings = new Vector<>();
		{
			firstRowEmbeddings.add(0.2);
			firstRowEmbeddings.add(0.4);
			firstRowEmbeddings.add(0.6);
			firstRowEmbeddings.add(0.8);
			allEmbeddings.add(firstRowEmbeddings);
		}

		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
				MockedStatic<QueryExecutionUtility> QEU = Mockito.mockStatic(QueryExecutionUtility.class);) {
			u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			EmbeddingsModelEngineResponse mockEmbeddingResponse = mock();
			when(modelEmbedder.embeddings(any(), any(), nullable(Map.class))).thenReturn(mockEmbeddingResponse);
			when(mockEmbeddingResponse.getResponse()).thenReturn(allEmbeddings);
			List<Map<String, Object>> output = new Vector<>();
			QEU.when(() -> QueryExecutionUtility.flushRsToMap(any(), any())).thenReturn(output);

			List<Map<String, Object>> nnCallOuput = engine.nearestNeighborCall(insight, searchStatement, limit,
					new HashMap<>());
			assertEquals(output, nnCallOuput);

			Properties updateEngineProps = engine.getSmssProp();
			assertNotNull(updateEngineProps);
			assertTrue(updateEngineProps.containsKey(Constants.MODEL));
			assertEquals(embedderModel, updateEngineProps.get(Constants.MODEL));
			assertTrue(updateEngineProps.containsKey(IModelEngine.MODEL_TYPE));
			assertEquals(embedderModelType, updateEngineProps.get(IModelEngine.MODEL_TYPE));
			assertTrue(updateEngineProps.containsKey(Constants.MAX_TOKENS));
			assertEquals("None", updateEngineProps.get(Constants.MAX_TOKENS));
			assertTrue(updateEngineProps.containsKey(Constants.MAX_TOKENS));
			assertEquals("", updateEngineProps.get(Constants.KEYWORD_ENGINE_ID));
		}
	}

	@Test
	void testNearestNeighborCallNoInsight() {
		Number limit = 1;
		String searchStatement = "searchStatement";
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> engine.nearestNeighborCall(null, searchStatement, limit, null));
		assertEquals("Insight must be provided to run Model Engine Encoder", e.getMessage());
	}

	@Test
	void testListDocuments() throws Exception {
		String indexClass = "index_class";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ENGINE_ALIAS";
		openEngine(tempDir, engine, null); // set initial properties

		Map<String, Object> parameters = new HashMap<>();
		parameters.put("indexClass", indexClass);

		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER)
				.resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path indexDirPath = Paths.get(engineFolder.toString(), "assets", "schema", indexClass);
		Files.createDirectories(indexDirPath);
		// create schema/index_class/documents
		Path docDirPath = indexDirPath.resolve(AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME);
		Files.createDirectories(docDirPath);
		// create 4 new files: newFile1 ... newFile4.txt
		List<String> fileNames = new Vector<>();
		for (int fileNum = 1; fileNum < 5; fileNum++) {
			String fileName = "newFile" + fileNum + ".txt";
			fileNames.add(fileName);
			Path newFilePath = docDirPath.resolve(fileName);
			Files.createFile(newFilePath);
		}

		try (MockedStatic<QueryExecutionUtility> QEU = Mockito.mockStatic(QueryExecutionUtility.class);) {
			List<Map<String, Object>> postgresDbOutput = new Vector<>();
			{
				for (String fileName : fileNames) {
					Map<String, Object> sourceMap = new HashMap<>();
					sourceMap.put("fileName", fileName);
					postgresDbOutput.add(sourceMap);
				}
			}

			QEU.when(() -> QueryExecutionUtility.flushRsToMap(any(), any())).thenReturn(postgresDbOutput);

			fileNames.forEach(
					fileName -> assertTrue(Files.exists(docDirPath.resolve(fileName)), fileName + " should exist"));
			List<Map<String, Object>> documentsList = engine.listDocuments(parameters);
			assertEquals(fileNames.size(), documentsList.size());
			for (int fileIdx = 0; fileIdx < fileNames.size(); fileIdx++) {
				String expectedFileName = fileNames.get(fileIdx);
				Map<String, Object> outputFileDetails = documentsList.get(fileIdx);
				assertEquals(expectedFileName, outputFileDetails.get("fileName"));
				assertEquals(0.0, outputFileDetails.get("fileSize"));
				LocalDateTime fileDateTime = LocalDateTime.parse((String) outputFileDetails.get("lastModified"),
						DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
				LocalDate fileDate = fileDateTime.toLocalDate();
				LocalDate todaysDate = LocalDate.now();
				assertEquals(todaysDate, fileDate); // checking date, omitting time
			}
		}
	}

	@Test
	void testListAllRecords() {
		List<Map<String, Object>> records = new Vector<>();
		Map<String, Object> record1 = new HashMap<>();
		{
			record1.put("key1", "value1");
			records.add(record1);
			records.add(record1);
			records.add(record1);
			records.add(record1);
		}
		try (MockedStatic<QueryExecutionUtility> QUE = Mockito.mockStatic(QueryExecutionUtility.class);) {
			QUE.when(() -> QueryExecutionUtility.flushRsToMap(any(IDatabaseEngine.class), any(SelectQueryStruct.class)))
					.thenReturn(records);
			List<Map<String, Object>> outputRecords = engine.listAllRecords(null);
			assertEquals(outputRecords.size(), records.size());
			// iterate over all records
			for (int idx = 0; idx < outputRecords.size(); idx++) {
				Map<String, Object> currOutputRecord = outputRecords.get(idx);
				Map<String, Object> expectedRecord = records.get(idx);
				assertEquals(currOutputRecord.size(), expectedRecord.size());
				// iterate over the current output record and compare contents
				for (Map.Entry<String, Object> recordEntry : currOutputRecord.entrySet()) {
					String currKey = recordEntry.getKey();
					Object currValue = recordEntry.getValue();
					assertEquals(expectedRecord.get(currKey), currValue);
				}
			}
		}
	}

	@Test
	void testGetCatalogType() {
		assertEquals(IEngine.CATALOG_TYPE.VECTOR, engine.getCatalogType());
	}

	@Test
	void testGetCatalogSubType() {
		String vectorDatabaseTypeStr = VectorDatabaseTypeEnum.PGVECTOR.toString();
		// smss properties can be null for this test
		assertEquals(vectorDatabaseTypeStr, engine.getCatalogSubType(null));
	}

	@Test
	void testAddDocument() throws Exception {
		String testEmbedderId = "123-456-789";
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties
		verifyModelProps(engine, testEmbedderId, embedderModel, embedderModelType);

		String indexClass = "index_class";

		Map<String, Object> parameters = new HashMap<>();
		parameters.put("indexClass", indexClass);
		parameters.put(Constants.INSIGHT, insight);

		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ENGINE_ALIAS";
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER)
				.resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path indexDirPath = Paths.get(engineFolder.toString(), "assets", "schema", indexClass);
		Files.createDirectories(indexDirPath);
		// create schema/index_class/documents
		Path docDirPath = indexDirPath.resolve(AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME);
		Files.createDirectories(docDirPath);
		// create schem/index_class/indexed_files
		Path indexFileDirPath = indexDirPath.resolve(AbstractVectorDatabaseEngine.INDEXED_FOLDER_NAME);
		Files.createDirectories(indexFileDirPath);

		// we need to create a few files in the "insight" folder
		Path insightDirPath = Paths.get(tempDir.toString(), "insight");
		Files.createDirectories(insightDirPath);
		// create 2 files for our insight folder
		String fileName1 = "newFile1.txt";
		String fileName2 = "newFile2.txt";
		Path file1 = insightDirPath.resolve(fileName1);
		Path file2 = insightDirPath.resolve(fileName2);
		Files.createFile(file1);
		Files.createFile(file2);
		List<String> fileNames = new Vector<>();
		fileNames.add(file1.toString());
		fileNames.add(file2.toString());

		SocketClient scMock = mock(SocketClient.class); // used in addDocument()->checkSocketStatus()->startServer()
		when(scMock.isConnected()).thenReturn(true);
		try (MockedConstruction<ClientProcessWrapper> mockWrapper = Mockito// used in
																			// addDocument()->checkSocketStatus()->startServer()
				.mockConstruction(ClientProcessWrapper.class, (mock, context) -> {
					doNothing().when(mock).createProcessAndClient(any(boolean.class), nullable(SymlinkHelper.class),
							any(int.class), nullable(String.class), nullable(String.class), nullable(String.class),
							any(boolean.class), any(String.class), any(String.class));
					doNothing().when(mock).shutdown(false);
					when(mock.getSocketClient()).thenReturn(scMock);

				});
				MockedConstruction<PyTranslator> mockPYT = Mockito.mockConstruction(PyTranslator.class, // used in
																										// addDocument()->checkSocketStatus()->startServer()
						(mock, context) -> {
							// doNothing().when(mock).setSocketClient(scMock);
							doNothing().when(mock).runEmptyPy(any());
							when(mock.runScript(any())).thenReturn("true");
						});
				MockedStatic<VectorDatabaseCSVTable> vdcsvt = Mockito.mockStatic(VectorDatabaseCSVTable.class);) {

			// used for addEmbeddings() portion
			VectorDatabaseCSVTable vectorCsvTableMock = mock();
			vdcsvt.when(() -> VectorDatabaseCSVTable.initCSVTable(any())).thenReturn(vectorCsvTableMock);
			try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);
					MockedStatic<AbstractSqlQueryUtil> asqu = Mockito.mockStatic(AbstractSqlQueryUtil.class);) {
				u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
				u.when(() -> Utility.normalizePath(file1.toString())).thenReturn(file1.toString());
				u.when(() -> Utility.normalizePath(file2.toString())).thenReturn(file2.toString());

				// used in getConnection()
				Connection mockConn = mock(Connection.class);
				asqu.when(() -> AbstractSqlQueryUtil.makeConnection(any(AbstractSqlQueryUtil.class), any(String.class),
						any(CaseInsensitiveProperties.class))).thenReturn(mockConn);
				PreparedStatement psMock = mock();
				when(mockConn.prepareStatement(any())).thenReturn(psMock);
				when(psMock.executeBatch()).thenReturn(new int[0]);

//				printDirContents(tempDir);
				assertTrue(Files.exists(file1));
				assertTrue(Files.exists(file2));
				engine.addDocument(fileNames, parameters);
//				printDirContents(tempDir);
				assertFalse(Files.exists(file1)); // deleted from insight folder
				assertFalse(Files.exists(file2)); // deleted from insight folder
				assertTrue(Files.exists(docDirPath.resolve(fileName1))); // moved to doc folder
				assertTrue(Files.exists(docDirPath.resolve(fileName2))); // moved to doc folder
			}
		}
	}

	/*
	 * Used for debugging file directory contents
	 */
	private void printDirContents(Path path) {
		System.out.println(path.toAbsolutePath());
		if (Files.isDirectory(path)) {
			try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
				for (Path entry : stream) {
					printDirContents(entry);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	void testAddDocumentNoInsight() throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties

		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		verifyModelProps(engine, testEmbedderId, embedderModel, embedderModelType);

		Map<String, Object> parameters = new HashMap<>();
		String indexClass = "TEST_INDEX_CLASS";
		parameters.put("indexClass", indexClass);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> engine.addDocument(new Vector<>(), parameters));
		assertEquals("Insight must be provided to run Model Engine Encoder", e.getMessage());
	}

	@Test
	void testGetIndexFilesPath() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> engine.getIndexFilesPath(null));
		assertEquals("Indexed files are not persisted for PGVector", e.getMessage());
	}

	@Test
	void testGetDocumentsFilesPath() throws Exception {
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ENGINE_ALIAS";
		String indexClass = "TEST_INDEX_CLASS";

		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER)
				.resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path indexDirPath = Paths.get(engineFolder.toString(), "assets", "schema", indexClass);
		Files.createDirectories(indexDirPath);
		Path docDirPath = indexDirPath.resolve(AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME);

		openEngine(tempDir, engine, null);
		assertEquals(Utility.normalizePath(docDirPath.toString()), engine.getDocumentsFilesPath(indexClass));
	}

	@Test
	void testGetDocumentsFilesPathInvalidDir() throws Exception {
		String nonExistantClass = "doesNotExist";
		openEngine(tempDir, engine, null); // adds default index to engine
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> engine.getDocumentsFilesPath(nonExistantClass));
		assertEquals("Unable to retieve document csv from a directory that does not exist", e.getMessage());
	}

	@Test
	void testUserCanAccessEmbeddingModels() throws Exception {
		String testEmbedderId = "123-456-789";
		String embedderModel = "embedder_model";
		String embedderModelType = "embedder_model_type";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties
		verifyModelProps(engine, testEmbedderId, embedderModel, embedderModelType);

		try (MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class);) {
			// used in userCanAccessEmbeddingModels
			seu.when(() -> SecurityEngineUtils.userCanViewEngine(user, testEmbedderId)).thenReturn(true);

			assertTrue(engine.userCanAccessEmbeddingModels(user));
			// verify added embedder engine props
			Properties updateEngineProps = engine.getSmssProp();
			assertNotNull(updateEngineProps);
			assertTrue(updateEngineProps.containsKey(Constants.MODEL));
			assertEquals(embedderModel, updateEngineProps.get(Constants.MODEL));
			assertTrue(updateEngineProps.containsKey(IModelEngine.MODEL_TYPE));
			assertEquals(embedderModelType, updateEngineProps.get(IModelEngine.MODEL_TYPE));
			assertTrue(updateEngineProps.containsKey(Constants.MAX_TOKENS));
			assertEquals("None", updateEngineProps.get(Constants.MAX_TOKENS));
			assertTrue(updateEngineProps.containsKey(Constants.MAX_TOKENS));
			assertEquals("", updateEngineProps.get(Constants.KEYWORD_ENGINE_ID));
		}
	}

	@Test
	void testVerifyModelPropsNoEmbedderId() throws Exception {
		openEngine(tempDir, engine, null); // set initial properties
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> engine.verifyModelProps());
		assertEquals(
				"Must define the embedder engine id for this vector database using " + Constants.EMBEDDER_ENGINE_ID,
				e.getMessage());
	}

	@Test
	void testVerifyModelPropsNoEmbedderEngine() throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties

		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);) {
			// used in verifyModelProps
			u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(null);

			NullPointerException e = assertThrows(NullPointerException.class, () -> engine.verifyModelProps());
			assertEquals("Could not find the defined embedder engine id for this vector database with value = "
					+ testEmbedderId, e.getMessage());
		}
	}

	@Test
	void testVerifyModelPropsNoModel() throws Exception {
		String testEmbedderId = "123-456-789";
		Map<String, String> extraProps = new HashMap<>();
		extraProps.put(Constants.EMBEDDER_ENGINE_ID, testEmbedderId);
		openEngine(tempDir, engine, extraProps); // set initial properties

		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);) {
			// used in verifyModelProps & addEmbeddings
			u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);

			// model embedder properties
			Properties embedderProps = new Properties();
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> engine.verifyModelProps());
			assertEquals("Embedder engine exists but does not contain key " + Constants.MODEL, e.getMessage());

		}
	}

	@Test
	void testGetVectorDatabaseType() {
		assertEquals(VectorDatabaseTypeEnum.PGVECTOR, engine.getVectorDatabaseType());
	}

	void openEngine(Path tempDir, PGVectorDatabaseEngine engine, Map<String, String> extraProps) throws Exception {
		Properties testProps = new Properties();
		String connPooling = "false";
		String testEngine = "asdf-1234";
		String testEngineAlias = "TEST_ENGINE_ALIAS";
		String testRDBMSType = "H2_DB";
		String url = "http://fake.url/";
		String testVectorTableName = "TEST_TABLE_NAME";
		String testKeepInputOutput = "false";
		String databaseZone = "America/Chicago";
		String owl = "REMAKE";
		testProps.setProperty(Constants.USE_CONNECTION_POOLING, connPooling);
		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(Constants.RDBMS_TYPE, testRDBMSType);
		testProps.setProperty("PGVECTOR_TABLE_NAME", testVectorTableName);
		testProps.setProperty(Constants.KEEP_INPUT_OUTPUT, testKeepInputOutput);
		testProps.setProperty(Constants.DATABASE_ZONEID, databaseZone);
		testProps.setProperty(Constants.OWL, owl);
		if (extraProps != null) {
			for (Map.Entry<String, String> extraPropsEntry : extraProps.entrySet()) {
				String key = extraPropsEntry.getKey();
				String prop = extraPropsEntry.getValue();
				testProps.setProperty(key, prop);
			}
		}

		String engineNameAndId = SmssUtilities.getUniqueName(testEngineAlias, testEngine);
		Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(engineNameAndId);
		Path engineAssetFolder = engineFolder.resolve("assets");
		Path engineVersionFolder = engineFolder.resolve("version");

		try (MockedStatic<SmssUtilities> su = Mockito.mockStatic(SmssUtilities.class);
				MockedStatic<SqlQueryUtilFactory> squf = Mockito.mockStatic(SqlQueryUtilFactory.class);
				MockedStatic<AbstractSqlQueryUtil> asqu = Mockito.mockStatic(AbstractSqlQueryUtil.class);
				MockedStatic<PGvector> pgv = Mockito.mockStatic(PGvector.class);) {
			// used in AbstractDatabaseEngine.open();
			su.when(() -> SmssUtilities.getDataFile(any())).thenReturn(null);
			su.when(() -> SmssUtilities.getUniqueName(any(), any())).thenCallRealMethod();
			// used in RDBMSNativeEngine.open()
			H2QueryUtil queryUtilMock = mock();
			{
				when(queryUtilMock.getConnectionUserKey()).thenReturn("user_key");
				when(queryUtilMock.getConnectionPasswordKey()).thenReturn("password_key");
				when(queryUtilMock.setConnectionDetailsFromSMSS(any())).thenReturn(null);
			}
			squf.when(() -> SqlQueryUtilFactory.initialize(any())).thenReturn(queryUtilMock);
			Connection mockConn = mock();
			asqu.when(() -> AbstractSqlQueryUtil.makeConnection(any(AbstractSqlQueryUtil.class), any(String.class),
					any(CaseInsensitiveProperties.class))).thenReturn(mockConn);
			// used in PGVectorDatabaseEngine.open()
			pgv.when(() -> PGvector.addVectorType(mockConn)).then(invocationOnMock -> null);
			Statement mockStm = mock(Statement.class);// RDBMSNativeEngine.open() -> used in initSQL() ->
														// execCreateStatement()
			when(mockStm.execute(any(String.class))).thenReturn(true);// RDBMSNativeEngine.open() -> used in initSQL()
																		// -> execCreateStatement()
			when(mockConn.createStatement()).thenReturn(mockStm);// RDBMSNativeEngine.open() -> used in initSQL() ->
																	// execCreateStatement()

			try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
				DIHelper diMock = mock(DIHelper.class);
				dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
				when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
				try (MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);
						MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);) {

					eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR,
							engineNameAndId)).thenReturn(engineFolder.toString());
					eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR,
							engineNameAndId)).thenReturn(engineAssetFolder.toString());
					eu.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.VECTOR,
							engineNameAndId)).thenReturn(engineVersionFolder.toString());
					eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, testEngine,
							testEngineAlias)).thenReturn(engineAssetFolder.toString());

					engine.open(testProps);
					assertTrue(Files.exists(engineAssetFolder));
					Properties engineProperties = engine.getSmssProp();
					assertNotNull(engineProperties);
					assertFalse(engineProperties.isEmpty());
					assertFalse(testProps.isEmpty());
					for (Entry<Object, Object> testProp : testProps.entrySet()) {
						assertTrue(engineProperties.containsKey(testProp.getKey()));
						assertTrue(engineProperties.containsValue(testProp.getValue()));
					}
					assertTrue(engineProperties.containsKey(Constants.CONNECTION_URL));
					assertTrue(engineProperties.containsValue(url));
				}
			}
		}
	}

	void verifyModelProps(PGVectorDatabaseEngine engine, String testEmbedderId, String embedderModel,
			String embedderModelType) {
		try (MockedStatic<Utility> u = Mockito.mockStatic(Utility.class);) {
			u.when(() -> Utility.getModel(testEmbedderId)).thenReturn(modelEmbedder);
			// model embedder properties
			Properties embedderProps = new Properties();
			embedderProps.setProperty(Constants.MODEL, embedderModel);
			embedderProps.setProperty(IModelEngine.MODEL_TYPE, embedderModelType);
			when(modelEmbedder.getSmssProp()).thenReturn(embedderProps);
			engine.verifyModelProps();
		}
	}
}
