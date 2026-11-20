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
package prerna.engine.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.openrdf.repository.RepositoryConnection;

import prerna.SemossUnitTest;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.owl.OWLEngineFactory;
import prerna.engine.impl.owl.ReadOnlyOWLEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.engine.impl.rdbms.AuditDatabase;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.io.connector.secrets.ISecrets;
import prerna.io.connector.secrets.SecretsFactory;
import prerna.query.interpreters.SparqlInterpreter;
import prerna.rdf.engine.wrappers.RawRDBMSSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.RDFEngineHelper;
import prerna.util.CSVToOwlMaker;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public class AbstractDatabaseEngineUnitTests extends SemossUnitTest {

	private AbstractDatabaseEngine engine;

	@BeforeEach
	public void setup() throws IOException {
		engine = new AbstractDatabaseEngine() {

			@Override
			public boolean holdsFileLocks() {
				return false;
			}

			@Override
			public Object execQuery(String query) throws Exception {
				return null;
			}

			@Override
			public void insertData(String query) throws Exception {

			}

			@Override
			public void removeData(String query) throws Exception {

			}

			@Override
			public void commit() {

			}

			@Override
			public DATABASE_TYPE getDatabaseType() {
				return DATABASE_TYPE.RDBMS;
			}

			@Override
			public Vector<Object> getEntityOfType(String type) {
				return null;
			}
		};

	}

	/// setDatabaseZoneId
	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { "NEVERLAND" })
	void testSetDbZoneIdNullOrEmpty(String src) {
		CaseInsensitiveProperties p = new CaseInsensitiveProperties();
		if (src != null) {
			p.put(Constants.DATABASE_ZONEID, src);
		}
		engine.setSmssProp(p);
		engine.setDatabaseZoneId();
		assertNull(engine.getDatabaseZoneId());
	}

	@ParameterizedTest
	@ValueSource(strings = { "UTC", "America/Chicago" })
	void testDbZoneID(String src) {
		CaseInsensitiveProperties p = new CaseInsensitiveProperties();
		p.put(Constants.DATABASE_ZONEID, src);
		engine.setSmssProp(p);
		engine.setDatabaseZoneId();
		assertEquals(ZoneId.of(src), engine.getDatabaseZoneId());
	}

	/// Generate OWL Flat file
	@Test
	void testGenerateOwlFromFlatFile() throws Exception {
		WriteOWLEngine writeOWLEngine = mock(WriteOWLEngine.class);
		try (MockedStatic<Utility> mockUtility = Mockito.mockStatic(Utility.class);
				MockedConstruction<CSVToOwlMaker> maker = Mockito.mockConstruction(CSVToOwlMaker.class,
						(mock, context) -> {
						});
				MockedConstruction<OWLEngineFactory> factory = Mockito.mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getWriteOWL()).thenReturn(writeOWLEngine);
						})) {

			RDFFileSesameEngine sesame = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(sesame);
			when(sesame.getEngineId()).thenReturn("engineId");

			engine.setEngineId("engineId");
			engine.setEngineName("engineName");

			engine.generateOwlFromFlatFile("data.txt", "owl.file", "owlFileName");

			assertEquals(1, maker.constructed().size());
			CSVToOwlMaker maker1 = maker.constructed().get(0);
			verify(maker1, times(1)).makeFlatOwl(writeOWLEngine, "data.txt", "owl.file",
					IDatabaseEngine.DATABASE_TYPE.RDBMS, true);
			mockUtility.verifyNoInteractions();
		}
	}

	@Test
	void testGenerateOwlFromFlatFileRemake() throws Exception {
		WriteOWLEngine writeOWLEngine = mock(WriteOWLEngine.class);
		try (MockedStatic<Utility> mockUtility = Mockito.mockStatic(Utility.class);
				MockedConstruction<CSVToOwlMaker> maker = Mockito.mockConstruction(CSVToOwlMaker.class,
						(mock, context) -> {
						});
				MockedConstruction<OWLEngineFactory> factory = Mockito.mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getWriteOWL()).thenReturn(writeOWLEngine);
						})) {

			RDFFileSesameEngine sesame = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(sesame);
			when(sesame.getEngineId()).thenReturn("engineId");

			engine.setEngineId("engineId");
			engine.setEngineName("engineName");

			engine.generateOwlFromFlatFile("REMAKE", "owl.file", "owlFileName");

			assertEquals(1, maker.constructed().size());
			CSVToOwlMaker maker1 = maker.constructed().get(0);
			verify(maker1, times(1)).makeFlatOwl(writeOWLEngine, "REMAKE", "owl.file",
					IDatabaseEngine.DATABASE_TYPE.RDBMS, true);
			mockUtility.verifyNoInteractions();
		}
	}

	/// Close
	@Test
	void closeDBsNotNull() throws IOException {
		try (MockedConstruction<OWLEngineFactory> factoryMock = mockConstruction(OWLEngineFactory.class);
				MockedConstruction<AuditDatabase> auditMock = mockConstruction(AuditDatabase.class)) {
			RDFFileSesameEngine rdf = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(rdf);
			engine.generateAudit();

			engine.close();

			verify(rdf, times(1)).close();
			AuditDatabase auditDatabase = auditMock.constructed().get(0);
			verify(auditDatabase, times(1)).close();
		}
	}

	@Test
	void closeDBsNull() throws IOException {
		engine.close();
		// nothing really to test other than that it works when things are null :)
		assertNull(engine.getBaseDataEngine());
	}

	/// getProperty
	@Test
	void generalAllNull() {
		engine.generalEngineProp = null;
		engine.ontoProp = null;
		engine.smssProp = null;
		assertNull(engine.getProperty("key"));
	}

	@Test
	void allMissingKey() {
		Properties general = new Properties();
		general.put("key", "general");
		Properties onto = new Properties();
		onto.put("key", "onto");
		CaseInsensitiveProperties smss = new CaseInsensitiveProperties();
		smss.put("key", "smss");
		engine.generalEngineProp = general;
		engine.ontoProp = onto;
		engine.smssProp = smss;
		assertNull(engine.getProperty("keyMissing"));
	}

	@Test
	void generalEnginePropContainsKeyAndTakesPriority() {
		Properties general = new Properties();
		general.put("key", "general");
		Properties onto = new Properties();
		onto.put("key", "onto");
		CaseInsensitiveProperties smss = new CaseInsensitiveProperties();
		smss.put("key", "smss");
		engine.generalEngineProp = general;
		engine.ontoProp = onto;
		engine.smssProp = smss;
		assertEquals("general", engine.getProperty("key"));
	}

	@Test
	void ontoPropertyWhenGeneralUnavailable() {
		Properties onto = new Properties();
		onto.put("key", "onto");
		CaseInsensitiveProperties smss = new CaseInsensitiveProperties();
		smss.put("key", "smss");
		engine.generalEngineProp = null;
		engine.ontoProp = onto;
		engine.smssProp = smss;
		assertEquals("onto", engine.getProperty("key"));
	}

	@Test
	void smssPropertyWhenGeneralUnavailable() {
		CaseInsensitiveProperties smss = new CaseInsensitiveProperties();
		smss.put("key", "smss");
		engine.generalEngineProp = null;
		engine.ontoProp = null;
		engine.smssProp = smss;
		assertEquals("smss", engine.getProperty("key"));
	}
	/// Simple methods

	@Test
	void testIsConnected() {
		assertFalse(engine.isConnected());
	}

	@Test
	void testSetEngineId() {
		engine.setEngineId("Test");
		assertEquals("Test", engine.getEngineId());
	}

	@Test
	void testGetMethodNameAddStatement() {
		assertEquals("addStatement", engine.getMethodName(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT));
	}

	@Test
	void testGetMethodNameRmStatement() {
		assertEquals("removeStatement", engine.getMethodName(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT));
	}

	@Test
	void addProperty() {
		engine.smssProp = new CaseInsensitiveProperties();
		engine.addProperty("foo", "bar");
		assertTrue(engine.smssProp.containsKey("foo"));
		assertEquals("bar", engine.smssProp.get("foo"));
	}

	@Test
	void setBaseHash() {
		engine.setEngineId("testid");
		Hashtable h = new Hashtable();
		h.put("foo", "bar");
		engine.setBaseHash(h);
		Hashtable h2 = engine.getBaseHash();
		assertEquals(1, h2.size());
		assertEquals("bar", h2.get("foo"));
	}

	/// /////////////////////////////////////////////////////
	/// TEST open
	/// /////////////////////////////////////////////////////

	@Nested
	class open {

		private Path tempPath;
		private Path testProps;
		private String testPropsPath;
		private File tempFile;
		private ISecrets secrets;

		private String owlFilePath;

		private MockedStatic<Utility> mockUtility = null;
		private MockedStatic<SecretsFactory> mockSecretsFactory = null;
		private MockedStatic<SmssUtilities> mockSmssUtilities = null;
		private MockedStatic<UploadUtilities> mockUploadUtilities = null;

		private List<MockedStatic> mockedStatics;

		@BeforeEach
		void setupForOpen() throws IOException {
			tempPath = tempDir;
			testProps = tempPath.resolve("test.props");
			testPropsPath = testProps.toAbsolutePath().toString();

			Path rdf = tempPath.resolve("RDF_map.prop");
			Properties prop = new Properties();
			try (OutputStream os = Files.newOutputStream(rdf)) {
				prop.store(os, null);
			}
			DIHelper.getInstance().loadCoreProp(rdf.toAbsolutePath().toString());

			// OWL File
			File generatedOwlPath = tempPath.resolve("generated.owl").toFile();
			owlFilePath = generatedOwlPath.getAbsolutePath();

			secrets = mock(ISecrets.class);

			mockedStatics = new ArrayList<>();

			mockUtility = Mockito.mockStatic(Utility.class);
			mockUtility.when(() -> Utility.loadProperties(any(String.class))).thenCallRealMethod();
			mockUtility.when(() -> Utility.normalizePath(any())).thenCallRealMethod();
			mockUtility.when(Utility::getBaseFolder).thenReturn(tempDir.toAbsolutePath().toString());
			mockUtility.when(() -> Utility.getDIHelperProperty(any())).thenCallRealMethod();
			mockedStatics.add(mockUtility);

			mockSecretsFactory = Mockito.mockStatic(SecretsFactory.class);
			mockedStatics.add(mockSecretsFactory);

			mockSmssUtilities = Mockito.mockStatic(SmssUtilities.class);
			mockSmssUtilities.when(() -> SmssUtilities.getUniqueName(anyString(), anyString())).thenCallRealMethod();
			mockedStatics.add(mockSmssUtilities);

			mockUploadUtilities = Mockito.mockStatic(UploadUtilities.class);
			mockUploadUtilities.when(() -> UploadUtilities.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, "testEngine",
					"testEngineAlias")).thenReturn(generatedOwlPath);

			mockedStatics.add(mockUploadUtilities);
		}

		@AfterEach
		public void teardown() {
			for (MockedStatic mockedStatic : mockedStatics) {
				if (mockedStatic != null) {
					mockedStatic.close();
				}
			}
		}

		@Test
		void testOpenSmssPropBasic() throws Exception {
			Properties p = new Properties();
			p.put(Constants.ENGINE, "testEngine");
			p.put(Constants.ENGINE_ALIAS, "testEngineAlias");
			p.put(Constants.DATABASE_ZONEID, "");
			try (OutputStream os = Files.newOutputStream(testProps)) {
				p.store(os, null);
			}

			engine.setBasic(true);

			engine.open(testPropsPath);

			assertEquals("testEngine", engine.getEngineId());
			assertEquals("testEngineAlias", engine.getEngineName());
			assertNull(engine.getDatabaseZoneId());
		}

		@Test
		void testOpenSmssPropBasicZoneIdCorrect() throws Exception {
			Properties p = new Properties();
			p.put(Constants.ENGINE, "testEngine");
			p.put(Constants.ENGINE_ALIAS, "testEngineAlias");
			p.put(Constants.DATABASE_ZONEID, "UTC");
			try (OutputStream os = Files.newOutputStream(testProps)) {
				p.store(os, null);
			}

			engine.setBasic(true);

			engine.open(testPropsPath);

			assertEquals("testEngine", engine.getEngineId());
			assertEquals("testEngineAlias", engine.getEngineName());
			assertEquals(ZoneId.of("UTC"), engine.getDatabaseZoneId());
		}

		/// ////////
		/// Opens secrets
		/// ///////

		@Test
		void testOpenSmssPropSecretsNull() throws Exception {
			RepositoryConnection rc = mock(RepositoryConnection.class);
			List<Object> args = new ArrayList<>();
			try (MockedStatic<RDFEngineHelper> rdfEngineHelper = Mockito.mockStatic(RDFEngineHelper.class);
					MockedConstruction<RDFFileSesameEngine> rdfFileSesameEngine = Mockito
							.mockConstruction(RDFFileSesameEngine.class, (mock, context) -> {
								when(mock.getRc()).thenReturn(rc);
							});
					MockedConstruction<OWLEngineFactory> owlEngineFactory = Mockito
							.mockConstruction(OWLEngineFactory.class, (mock, context) -> {
								args.add(context.arguments().get(0));
								args.add(context.arguments().get(1));
								args.add(context.arguments().get(2));
								args.add(context.arguments().get(3));
							})) {

				Properties p = new Properties();
				p.put(Constants.ENGINE, "testEngine");
				p.put(Constants.ENGINE_ALIAS, "testEngineAlias");
				p.put(Constants.DATABASE_ZONEID, "UTC");
				try (OutputStream os = Files.newOutputStream(testProps)) {
					p.store(os, null);
				}

				mockSecretsFactory.when(SecretsFactory::getSecretConnector).thenReturn(null);

				mockSmssUtilities.when(() -> SmssUtilities.getEngineProperties(any(CaseInsensitiveProperties.class)))
						.thenReturn(null);

				Hashtable<Object, Object> baseHash = new Hashtable<>();
				rdfEngineHelper.when(() -> RDFEngineHelper.createBaseFilterHash(rc)).thenReturn(baseHash);

				engine.open(testPropsPath);

				assertEquals("testEngine", engine.getEngineId());
				assertEquals("testEngineAlias", engine.getEngineName());
				assertEquals(ZoneId.of("UTC"), engine.getDatabaseZoneId());

				assertEquals(owlFilePath, engine.getOwlFilePath());

				assertEquals(4, engine.getSmssProp().size());

				assertNotNull(engine.getOWLEngineFactory());
				assertNotNull(args.get(0));
				assertEquals(IDatabaseEngine.DATABASE_TYPE.RDBMS, args.get(1));
				assertEquals("testEngine", args.get(2));
				assertEquals("testEngineAlias", args.get(3));

				assertNull(engine.generalEngineProp);
			}
		}
	}

	/// CreateBaseRelationEngine
	@Nested
	class CreateBaseRelationEngine {

		@Test
		void create() throws Exception {
			Path base = tempDir.resolve("Semoss");
			Files.createDirectories(base);
			Properties rdf = new Properties();
			rdf.put(Constants.BASE_FOLDER, tempDir.toAbsolutePath().toString());
			DIHelper.getInstance().setCoreProp(rdf);

			RepositoryConnection mockRC = mock(RepositoryConnection.class);
			try (MockedConstruction<OWLEngineFactory> mockedOwlEngineFactory = mockConstruction(OWLEngineFactory.class);
					MockedStatic<EngineUtility> mockEngineUtility = mockStatic(EngineUtility.class);
					MockedConstruction<RDFFileSesameEngine> mockConstructionSesame = mockConstruction(
							RDFFileSesameEngine.class, (mock, context) -> {
								when(mock.getRc()).thenReturn(mockRC);
							});
					MockedStatic<RDFEngineHelper> mockRDFEngineHelper = mockStatic(RDFEngineHelper.class)) {
				RDFFileSesameEngine rdfFileSesameEngine = mock(RDFFileSesameEngine.class);
				engine.setBaseDataEngine(rdfFileSesameEngine);

				engine.setEngineId("testId");
				engine.setEngineName("testEngine");

				CaseInsensitiveProperties p = new CaseInsensitiveProperties();
				engine.smssProp = p;

				mockEngineUtility.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.DATABASE,
						"testId", "testEngine")).thenReturn("testFileLocation");

				Hashtable h = new Hashtable();
				h.put("foo", "bar");
				mockRDFEngineHelper.when(() -> RDFEngineHelper.createBaseFilterHash(mockRC)).thenReturn(h);

				engine.createBaseRelationEngine();

				verify(rdfFileSesameEngine, times(1)).close();
				RDFFileSesameEngine created = mockConstructionSesame.constructed().get(0);

				assertTrue(engine.smssProp.getProperty(Constants.OWL).startsWith("testFileLocation"));
				assertTrue(engine.smssProp.getProperty(Constants.OWL).endsWith("testEngine_OWL.OWL"));
				verify(created, times(1)).setBasic(true);
				verify(created, times(1)).open(any(Properties.class));
				verify(created, times(1)).commit();
			}
		}
	}

	@Test
	void getFromNeighborsNull() {
		assertNull(engine.getFromNeighbors("test", 1));
		assertNull(engine.getOWLEngineFactory());
	}

	@Test
	void getToNeighborsNull() {
		assertNull(engine.getToNeighbors("test", 1));
		assertNull(engine.getOWLEngineFactory());
	}

	@Test
	void getNeighborsNull() {
		assertNull(engine.getNeighbors("test", 1));
		assertNull(engine.getOWLEngineFactory());
	}

	@Test
	void testSetOwlFilePath() throws IOException {
		Path base = tempDir.resolve("Semoss");
		Path p = base.resolve("test.owl");
		String owlpath = p.toAbsolutePath().toString();

		engine.setOwlFilePath(owlpath);
		Files.createDirectories(base);
		Properties rdf = new Properties();
		rdf.put(Constants.BASE_FOLDER, tempDir.toAbsolutePath().toString());
		DIHelper.getInstance().setCoreProp(rdf);

		RepositoryConnection mockRC = mock(RepositoryConnection.class);
		try (MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class);
				MockedStatic<EngineUtility> mockEngineUtility = mockStatic(EngineUtility.class);
				MockedConstruction<RDFFileSesameEngine> mockConstructionSesame = mockConstruction(
						RDFFileSesameEngine.class, (mock, context) -> {
							when(mock.getRc()).thenReturn(mockRC);
						});
				MockedStatic<RDFEngineHelper> mockRDFEngineHelper = mockStatic(RDFEngineHelper.class)) {
			RDFFileSesameEngine rdfFileSesameEngine = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(rdfFileSesameEngine);

			engine.setEngineId("testId");
			engine.setEngineName("testEngine");

			mockEngineUtility.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.DATABASE,
					"testId", "testEngine")).thenReturn("testFileLocation");

			Hashtable h = new Hashtable();
			h.put("foo", "bar");
			mockRDFEngineHelper.when(() -> RDFEngineHelper.createBaseFilterHash(mockRC)).thenReturn(h);

			engine.setOwlFilePath(owlpath);

			// verify createBaseRelationEngine is called
			verify(rdfFileSesameEngine, times(1)).close();
			RDFFileSesameEngine created = mockConstructionSesame.constructed().get(0);

			verify(created, times(1)).setBasic(true);
			verify(created, times(1)).setFilePath(owlpath);
			verify(created, times(1)).commit();
		}

		assertEquals(owlpath, engine.getOwlFilePath());
		assertNotNull(engine.getOWLEngineFactory());
	}

	@Test
	void testSmssFilePath() {
		engine.setSmssFilePath("test");
		assertEquals("test", engine.getSmssFilePath());
	}

	@Test
	void testGetOWLDefinitionNull() {
		assertNull(engine.getOWLDefinition());
		assertNull(engine.getOWLEngineFactory());
	}

	@Test
	void testGetOWLDefinition() {
		ReadOnlyOWLEngine owl = mock(ReadOnlyOWLEngine.class);
		when(owl.getOWLDefinition()).thenReturn("OWL DEF");
		try (MockedStatic<Utility> ignored = Mockito.mockStatic(Utility.class);
				MockedConstruction<CSVToOwlMaker> ignored2 = Mockito.mockConstruction(CSVToOwlMaker.class);
				MockedConstruction<OWLEngineFactory> ignored1 = Mockito.mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(owl);
						})) {

			RDFFileSesameEngine sesame = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(sesame);
			when(sesame.getEngineId()).thenReturn("engineId");

			engine.setEngineId("engineId");
			engine.setEngineName("engineName");

			String def = engine.getOWLDefinition();
			assertEquals("OWL DEF", def);
		}
	}

	@Test
	void testGetQueryInterpreter() {
		SparqlInterpreter sparqlInterpreter = (SparqlInterpreter) engine.getQueryInterpreter();
		assertNotNull(sparqlInterpreter);
	}

	@Test
	void testCommitOwl() {
		try (MockedStatic<Utility> ignored = Mockito.mockStatic(Utility.class);
				MockedConstruction<CSVToOwlMaker> ignored2 = Mockito.mockConstruction(CSVToOwlMaker.class);
				MockedConstruction<OWLEngineFactory> ignored1 = Mockito.mockConstruction(OWLEngineFactory.class)) {

			RDFFileSesameEngine sesame = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(sesame);
			when(sesame.getEngineId()).thenReturn("engineId");

			engine.setEngineId("engineId");
			engine.setEngineName("engineName");

			engine.commitOWL();

			verify(sesame, times(1)).commit();
		}
	}

	@Test
	void testGetConceptsNull() {
		assertNull(engine.getConcepts());
		assertNull(engine.getOWLEngineFactory());
	}

	@Test
	void getConcepts() {
		ReadOnlyOWLEngine owl = mock(ReadOnlyOWLEngine.class);
		Vector<String> vec = new Vector<>();
		vec.add("testone");
		vec.add("testtwo");
		when(owl.getConcepts()).thenReturn(vec);
		try (MockedStatic<Utility> ignored = Mockito.mockStatic(Utility.class);
				MockedConstruction<CSVToOwlMaker> ignored2 = Mockito.mockConstruction(CSVToOwlMaker.class);
				MockedConstruction<OWLEngineFactory> ignored1 = Mockito.mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(owl);
						})) {

			RDFFileSesameEngine sesame = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(sesame);
			when(sesame.getEngineId()).thenReturn("engineId");

			engine.setEngineId("engineId");
			engine.setEngineName("engineName");

			Vector<String> concepts = engine.getConcepts();
			assertEquals(2, concepts.size());
			assertEquals("testone", concepts.get(0));
			assertEquals("testtwo", concepts.get(1));
		}
	}

	static Stream<Arguments> methodNamesProvider() {
		return Stream.of(Arguments.of(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, "addStatement"),
				Arguments.of(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, "removeStatement"),
				Arguments.of(IDatabaseEngine.ACTION_TYPE.BULK_INSERT, "bulkInsertPreparedStatement"),
				Arguments.of(IDatabaseEngine.ACTION_TYPE.VERTEX_UPSERT, "upsertVertex"),
				Arguments.of(IDatabaseEngine.ACTION_TYPE.EDGE_UPSERT, "upsertEdge"));
	}

	@ParameterizedTest
	@MethodSource("methodNamesProvider")
	void getMethodName(IDatabaseEngine.ACTION_TYPE actionType, String expected) {
		assertEquals(expected, engine.getMethodName(actionType));
	}

	@Test
	void makeSureGetMethodNameHasOptionForEachActionType() {
		assertEquals(5, IDatabaseEngine.ACTION_TYPE.values().length, "IF THIS TEST FAILS, PLEASE ADD"
				+ "AN OPTION TO THE SWITCH STATEMENT. IF IT DOESN'T NEED AN OPTION IN THE METHOD AbstractDatabaseEngine.getMethodName()"
				+ "SET 5 TO THE CORRECT VALUE.");
	}

	@ParameterizedTest
	@EnumSource(IDatabaseEngine.ACTION_TYPE.class)
	void testDoAction(IDatabaseEngine.ACTION_TYPE actionType) {
		// AbstractDatabaseEngine has none of these methods. All should return null.
		Object[] args = new Object[] {};
		assertNull(engine.doAction(actionType, args));
	}

	@Nested
	class Delete {

		@Test
		void testDelete() throws Exception {
			FileUtils.cleanDirectory(tempDir.toFile());
			String engineId = "testId";
			String engineName = "testEngine";
			engine.setEngineId(engineId);
			engine.setEngineName(engineName);

			String engineNameAndId = SmssUtilities.getUniqueName(engineName, engineId);

			Path base = tempDir.resolve("Semoss");
			Path engineDir = base.resolve("db");
			Path testEngine = engineDir.resolve("testEngine__testId");
			Files.createDirectories(testEngine);
			Path owlPath = testEngine.resolve("testEngine__testId.owl");
			Files.createFile(owlPath);
			String owlPathString = owlPath.toAbsolutePath().toString();

			Path engineSMSS = engineDir.resolve("testEngine__testId.smss");
			Files.createFile(engineSMSS);
			engine.setSmssFilePath(engineSMSS.toAbsolutePath().toString());

			engine.setOwlFilePath(owlPathString);

			Properties testProps = new Properties();
			testProps.setProperty(Constants.ENGINE, "testId");
			testProps.setProperty(Constants.ENGINE_ALIAS, "testEngine");

			Properties rdf = new Properties();
			rdf.put(Constants.BASE_FOLDER, base.toAbsolutePath().toString());
			DIHelper.getInstance().setCoreProp(rdf);
			DIHelper.getInstance().setEngineProperty(Constants.ENGINES, "testId;");
			DIHelper.getInstance().setEngineProperty("testId_" + Constants.OWL, "value");
			DIHelper.getInstance().setEngineProperty("testId_" + Constants.STORE, "value");
			DIHelper.getInstance().setEngineProperty("testId", "value");

			Path engineFolder = engineDir.resolve(engineNameAndId);
			Path engineAssetFolder = engineFolder.resolve("assets");
			Path engineVersionFolder = engineFolder.resolve("version");

			RepositoryConnection mockRC = mock(RepositoryConnection.class);
			try (MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class);
					MockedConstruction<RDFFileSesameEngine> mockConstructionSesame = mockConstruction(
							RDFFileSesameEngine.class, (mock, context) -> {
								when(mock.getRc()).thenReturn(mockRC);
							});
					MockedStatic<EngineUtility> mockedEngineUtility = mockStatic(EngineUtility.class)) {
				RDFFileSesameEngine rdfFileSesameEngine = mock(RDFFileSesameEngine.class);
				engine.setBaseDataEngine(rdfFileSesameEngine);

				mockedEngineUtility.when(
						() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.DATABASE, engineNameAndId))
						.thenReturn(testEngine.toAbsolutePath().toString());

				mockedEngineUtility.when(
						() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.DATABASE, engineNameAndId))
						.thenReturn(engineFolder.toString());
				mockedEngineUtility.when(() -> EngineUtility
						.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.DATABASE, engineNameAndId))
						.thenReturn(engineAssetFolder.toString());
				mockedEngineUtility.when(() -> EngineUtility
						.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.DATABASE, engineNameAndId))
						.thenReturn(engineVersionFolder.toString());

				mockedEngineUtility.when(() -> EngineUtility
						.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.DATABASE, engineId, engineName))
						.thenReturn(engineAssetFolder.toString());

				engine.open(testProps);
				engine.delete();

				verify(rdfFileSesameEngine, times(1)).close();
				assertTrue(Files.notExists(owlPath));
				assertTrue(Files.notExists(engineSMSS));
				assertTrue(Files.notExists(testEngine));
				assertEquals("", DIHelper.getInstance().getEngineProperty(Constants.ENGINES));
				assertNull(DIHelper.getInstance().getEngineProperty("testId"));
				assertNull(DIHelper.getInstance().getEngineProperty("testId_" + Constants.OWL));
				assertNull(DIHelper.getInstance().getEngineProperty("testId_" + Constants.STORE));
			}
		}
	}

	@Test
	void executeInsightQuery() {
		try (MockedStatic<Utility> utilMockedStatic = Mockito.mockStatic(Utility.class)) {
			Vector<String> queryRet = new Vector<>();
			queryRet.add("test");
			utilMockedStatic.when(() -> Utility.getVectorOfReturn("query", engine, true)).thenReturn(queryRet);

			Vector<String> result = engine.executeInsightQuery("query", true);
			assertEquals(1, result.size());
			assertEquals("test", result.get(0));
		}
	}

	@Test
	void executeInsightQueryNotDB() {
		RepositoryConnection mockRC = mock(RepositoryConnection.class);
		try (MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class);
				MockedConstruction<RDFFileSesameEngine> mockConstructionSesame = mockConstruction(
						RDFFileSesameEngine.class, (mock, context) -> {
							when(mock.getRc()).thenReturn(mockRC);
						});
				MockedStatic<Utility> utilMockedStatic = Mockito.mockStatic(Utility.class)) {
			RDFFileSesameEngine rdfFileSesameEngine = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(rdfFileSesameEngine);

			Vector<String> queryRet = new Vector<>();
			queryRet.add("test");
			utilMockedStatic.when(() -> Utility.getVectorOfReturn("query", rdfFileSesameEngine, true))
					.thenReturn(queryRet);

			// test to make sure we're querying against base data engine
			Vector<String> result = engine.executeInsightQuery("query", false);
			assertEquals(1, result.size());
			assertEquals("test", result.get(0));
		}
	}

	@Test
	void isBasic() {
		engine.setBasic(false);
		assertFalse(engine.isBasic());
	}

	@Test
	void getNodeBaseUriFromWrapper() throws Exception {
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class)) {
			RDFFileSesameEngine rdfFileSesameEngine = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(rdfFileSesameEngine);

			WrapperManager wm = mock(WrapperManager.class);
			mockedWM.when(WrapperManager::getInstance).thenReturn(wm);

			IRawSelectWrapper wrapper = mock(RawRDBMSSelectWrapper.class);
			when(wrapper.hasNext()).thenReturn(true);
			IHeadersDataRow row = mock(IHeadersDataRow.class);
			when(row.getRawValues()).thenReturn(new Object[] { "baseUri" });
			when(wrapper.next()).thenReturn(row);
			when(wm.getRawWrapper(rdfFileSesameEngine,
					"SELECT DISTINCT ?entity WHERE { { <SEMOSS:ENGINE_METADATA> <CONTAINS:BASE_URI> ?entity } } LIMIT 1"))
					.thenReturn(wrapper);

			String response = engine.getNodeBaseUri();

			assertEquals("baseUri", response);
			verify(wrapper, times(1)).close();
		}
	}

	@Test
	void getNodeBaseUriFromWrapperNull() throws Exception {
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class)) {
			RDFFileSesameEngine rdfFileSesameEngine = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(rdfFileSesameEngine);

			WrapperManager wm = mock(WrapperManager.class);
			mockedWM.when(WrapperManager::getInstance).thenReturn(wm);

			IRawSelectWrapper wrapper = mock(RawRDBMSSelectWrapper.class);
			when(wrapper.hasNext()).thenReturn(false);
			when(wm.getRawWrapper(rdfFileSesameEngine,
					"SELECT DISTINCT ?entity WHERE { { <SEMOSS:ENGINE_METADATA> <CONTAINS:BASE_URI> ?entity } } LIMIT 1"))
					.thenReturn(wrapper);

			String response = engine.getNodeBaseUri();

			assertEquals("http://semoss.org/ontologies/Concept/", response);
			verify(wrapper, times(1)).close();
		}
	}

	@Test
	void getDataTypesNull() {
		assertNull(engine.getDataTypes("any"));
		assertNull(engine.getOWLEngineFactory());
	}

	@Test
	void getDataTypes() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			when(reoe.getDataTypes("any")).thenReturn("val");

			String type = engine.getDataTypes("any");

			assertEquals("val", type);
		}
	}

	@Test
	void getDataTypesMap() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			Map<String, String> test = new HashMap<>();
			test.put("test", "test");
			when(reoe.getDataTypes("any", "any2")).thenReturn(test);

			Map<String, String> ret = engine.getDataTypes("any", "any2");

			assertEquals("test", ret.get("test"));
		}
	}

	@Test
	void getAdtlDataTypes() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			when(reoe.getAdtlDataTypes("any")).thenReturn("val");

			String type = engine.getAdtlDataTypes("any");

			assertEquals("val", type);
		}
	}

	@Test
	void getAdtlDataTypesMap() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			Map<String, String> test = new HashMap<>();
			test.put("test", "test");
			when(reoe.getAdtlDataTypes("any", "any2")).thenReturn(test);

			Map<String, String> ret = engine.getAdtlDataTypes("any", "any2");

			assertEquals("test", ret.get("test"));
		}
	}

	@Test
	void getMetaModel() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			Map<String, Object[]> test = new HashMap<>();
			test.put("test", new Object[] { "val" });
			when(reoe.getMetamodel()).thenReturn(test);

			Map<String, Object[]> ret = engine.getMetamodel();

			assertEquals("val", ret.get("test")[0].toString());
		}
	}

	@Test
	void getOwlPositionFile() throws IOException {
		Path p = tempDir.resolve("test.owl");
		Path positions = tempDir.resolve("positions.json");
		engine.setOwlFilePath(p.toAbsolutePath().toString());
		Files.createFile(positions);
		File f = engine.getOwlPositionFile();
		assertTrue(f.exists());
	}

	@Test
	void getPixelConcepts() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			List<String> concepts = new ArrayList<>();
			concepts.add("concept");
			when(reoe.getPixelConcepts()).thenReturn(concepts);

			List<String> ret = engine.getPixelConcepts();

			assertEquals("concept", ret.get(0));
		}
	}

	@Test
	void getPixelSelectors() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			List<String> selectors = new ArrayList<>();
			selectors.add("select");
			when(reoe.getPixelSelectors("test")).thenReturn(selectors);

			List<String> ret = engine.getPixelSelectors("test");

			assertEquals("select", ret.get(0));
			assertEquals(1, ret.size());
		}
	}

	@Test
	void getPropertyPixelSelectors() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			List<String> selectors = new ArrayList<>();
			selectors.add("select");
			when(reoe.getPropertyPixelSelectors("test")).thenReturn(selectors);

			List<String> ret = engine.getPropertyPixelSelectors("test");

			assertEquals("select", ret.get(0));
			assertEquals(1, ret.size());
		}
	}

	@Test
	void getPhysicalConcepts() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			List<String> selectors = new ArrayList<>();
			selectors.add("select");
			when(reoe.getPhysicalConcepts()).thenReturn(selectors);

			List<String> ret = engine.getPhysicalConcepts();

			assertEquals("select", ret.get(0));
			assertEquals(1, ret.size());
		}
	}

	@Test
	void getPhysicalRelationships() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			List<String[]> selectors = new ArrayList<>();
			selectors.add(new String[] { "select" });
			when(reoe.getPhysicalRelationships()).thenReturn(selectors);

			List<String[]> ret = engine.getPhysicalRelationships();

			assertEquals("select", ret.get(0)[0]);
			assertEquals(1, ret.size());
		}
	}

	@Test
	void getPropertyUris4PhysicalUri() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			List<String> selectors = new ArrayList<>();
			selectors.add("select");
			when(reoe.getPropertyUris4PhysicalUri("test")).thenReturn(selectors);

			List<String> ret = engine.getPropertyUris4PhysicalUri("test");

			assertEquals("select", ret.get(0));
			assertEquals(1, ret.size());
		}
	}

	@Test
	void getPhysicalUriFromPixelSelector() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			when(reoe.getPhysicalUriFromPixelSelector("test")).thenReturn("test");

			String ret = engine.getPhysicalUriFromPixelSelector("test");

			assertEquals("test", ret);
		}
	}

	@Test
	void getPixelUriFromPhysicalUri() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			when(reoe.getPixelUriFromPhysicalUri("test")).thenReturn("test");

			String ret = engine.getPixelUriFromPhysicalUri("test");

			assertEquals("test", ret);
		}
	}

	@Test
	void getConceptPixelUriFromPhysicalUri() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			when(reoe.getConceptPixelUriFromPhysicalUri("test")).thenReturn("test");

			String ret = engine.getConceptPixelUriFromPhysicalUri("test");

			assertEquals("test", ret);
		}
	}

	@Test
	void getPropertyPixelUriFromPhysicalUri() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			when(reoe.getPropertyPixelUriFromPhysicalUri("concept", "physical")).thenReturn("test");

			String ret = engine.getPropertyPixelUriFromPhysicalUri("concept", "physical");

			assertEquals("test", ret);
		}
	}

	@Test
	void getPixelSelectorFromPhysicalUri() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			when(reoe.getPixelSelectorFromPhysicalUri("test")).thenReturn("test");

			String ret = engine.getPixelSelectorFromPhysicalUri("test");

			assertEquals("test", ret);
		}
	}

	@Test
	void getConceptualName() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			when(reoe.getConceptualName("test")).thenReturn("test");

			String ret = engine.getConceptualName("test");

			assertEquals("test", ret);
		}
	}

	@Test
	void getLogicalNames() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			Set<String> val = new HashSet<>();
			val.add("test");
			when(reoe.getLogicalNames("test")).thenReturn(val);

			Set<String> ret = engine.getLogicalNames("test");

			assertEquals("test", ret.stream().findFirst().orElse("wrong"));
		}
	}

	@Test
	void getDescription() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			when(reoe.getDescription("test")).thenReturn("test");

			String ret = engine.getDescription("test");

			assertEquals("test", ret);
		}
	}

	@Test
	void getLegacyPrimKey4Table() {
		ReadOnlyOWLEngine reoe = mock(ReadOnlyOWLEngine.class);
		try (MockedStatic<WrapperManager> mockedWM = Mockito.mockStatic(WrapperManager.class);
				MockedConstruction<OWLEngineFactory> ignored = mockConstruction(OWLEngineFactory.class,
						(mock, context) -> {
							when(mock.getReadOWL()).thenReturn(reoe);
						})) {
			RDFFileSesameEngine mocked = mock(RDFFileSesameEngine.class);
			engine.setBaseDataEngine(mocked);

			when(reoe.getLegacyPrimKey4Table("test")).thenReturn("test");

			String ret = engine.getLegacyPrimKey4Table("test");

			assertEquals("test", ret);
		}
	}

	@Test
	void getUDF() {
		Properties p = new Properties();
		p.put("UDF", "one;two");
		engine.setSmssProp(p);
		String[] val = engine.getUDF();
		assertEquals("one", val[0]);
		assertEquals("two", val[1]);
	}

	@Test
	void getCatalogSubType() {
		Properties p = new Properties();
		assertEquals("RDBMS", engine.getCatalogSubType(p));
	}
}
