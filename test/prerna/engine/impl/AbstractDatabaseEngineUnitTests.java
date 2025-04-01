package prerna.engine.impl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.openrdf.repository.RepositoryConnection;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.impl.owl.OWLEngineFactory;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.io.connector.secrets.ISecrets;
import prerna.io.connector.secrets.SecretsFactory;
import prerna.ui.components.RDFEngineHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

import java.io.File;
import java.time.ZoneId;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class AbstractDatabaseEngineUnitTests {

    private AbstractDatabaseEngine engine;

    @BeforeEach
    public void setup() {
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


    ////////////////////////////////////////////////////////
    /// TEST open
    ////////////////////////////////////////////////////////

    @Test
    void testOpenSmssPropEmpty() throws Exception {
        try (MockedStatic<Utility> mockedUtility = Mockito.mockStatic(Utility.class)) {
            Properties p = mock(Properties.class);
            when(p.isEmpty()).thenReturn(true);
            mockedUtility.when(() -> Utility.loadProperties("path")).thenReturn(p);

            engine.open("path");

            verify(p, times(0)).getProperty(any());
            Assertions.assertInstanceOf(CaseInsensitiveProperties.class, engine.getSmssProp());
            assertNull(engine.getEngineId());
            assertNull(engine.getEngineName());
            assertNull(engine.getDatabaseZoneId());
        }
    }

    @Test
    void testOpenSmssPropEmptyCaseInsensitive() throws Exception {
        try (MockedStatic<Utility> mockedUtility = Mockito.mockStatic(Utility.class)) {
            Properties p = new CaseInsensitiveProperties();
            mockedUtility.when(() -> Utility.loadProperties("path")).thenReturn(p);

            engine.open("path");

            Assertions.assertInstanceOf(CaseInsensitiveProperties.class, engine.getSmssProp());
            assertEquals(p, engine.getOrigSmssProp());
            assertNull(engine.getEngineId());
            assertNull( engine.getEngineName());
            assertNull(engine.getDatabaseZoneId());
        }

    }


    @Test
    void testOpenSmssPropBasic() throws Exception {
        try (MockedStatic<Utility> mockedUtility = Mockito.mockStatic(Utility.class);
             MockedStatic<SmssUtilities> smsUtilities = Mockito.mockStatic(SmssUtilities.class);
             MockedStatic<SecretsFactory> sf = Mockito.mockStatic(SecretsFactory.class)) {

            Properties p = new Properties();
            p.put(Constants.ENGINE, "testEngine");
            p.put(Constants.ENGINE_ALIAS, "testEngineAlias");
            p.put(Constants.DATABASE_ZONEID, "");
            mockedUtility.when(() -> Utility.loadProperties("path")).thenReturn(p);
            smsUtilities.when(() -> SmssUtilities.getUniqueName("testEngineAlias", "testEngine"))
                    .thenReturn("testUniqueEngineName");


            ISecrets secrets = mock(ISecrets.class);
            sf.when(SecretsFactory::getSecretConnector).thenReturn(secrets);


            engine.setBasic(true);

            engine.open("path");

            verifyNoInteractions(secrets);
            assertEquals("testEngine", engine.getEngineId());
            assertEquals("testEngineAlias", engine.getEngineName());
            assertNull(engine.getDatabaseZoneId());
        }
    }


    @Test
    void testOpenSmssPropBasicZoneIdCorrect() throws Exception {
        try (MockedStatic<Utility> mockedUtility = Mockito.mockStatic(Utility.class);
             MockedStatic<SmssUtilities> smsUtilities = Mockito.mockStatic(SmssUtilities.class);
             MockedStatic<SecretsFactory> sf = Mockito.mockStatic(SecretsFactory.class)) {

            Properties p = new Properties();
            p.put(Constants.ENGINE, "testEngine");
            p.put(Constants.ENGINE_ALIAS, "testEngineAlias");
            p.put(Constants.DATABASE_ZONEID, "UTC");
            mockedUtility.when(() -> Utility.loadProperties("path")).thenReturn(p);
            smsUtilities.when(() -> SmssUtilities.getUniqueName("testEngineAlias", "testEngine"))
                    .thenReturn("testUniqueEngineName");


            ISecrets secrets = mock(ISecrets.class);
            sf.when(SecretsFactory::getSecretConnector).thenReturn(secrets);


            engine.setBasic(true);

            engine.open("path");

            assertEquals("testEngine", engine.getEngineId());
            assertEquals("testEngineAlias", engine.getEngineName());
            assertEquals(ZoneId.of("UTC"), engine.getDatabaseZoneId());
            verifyNoInteractions(secrets);
        }
    }

    ///////////
    /// Opens secrets
    //////////

    @Test
    void testOpenSmssPropSecretsNull() throws Exception {
        RepositoryConnection rc = mock(RepositoryConnection.class);
        List<Object> args = new ArrayList<>();
        try (MockedStatic<Utility> mockedUtility = Mockito.mockStatic(Utility.class);
             MockedStatic<SmssUtilities> smsUtilities = Mockito.mockStatic(SmssUtilities.class);
             MockedStatic<SecretsFactory> sf = Mockito.mockStatic(SecretsFactory.class);
             MockedStatic<UploadUtilities> uu = Mockito.mockStatic(UploadUtilities.class);
             MockedStatic<RDFEngineHelper> rdfEngineHelper = Mockito.mockStatic(RDFEngineHelper.class);
             MockedConstruction<RDFFileSesameEngine> rdfFileSesameEngine = Mockito.mockConstruction(RDFFileSesameEngine.class, (mock, context) -> {
                 when(mock.getRc()).thenReturn(rc);
             });
             MockedConstruction<OWLEngineFactory> owlEngineFactory = Mockito.mockConstruction(OWLEngineFactory.class, (mock, context) -> {
                 args.add(context.arguments().get(0));
                 args.add(context.arguments().get(1));
                 args.add(context.arguments().get(2));
                 args.add(context.arguments().get(3));
             })) {

            Properties p = new Properties();
            p.put(Constants.ENGINE, "testEngine");
            p.put(Constants.ENGINE_ALIAS, "testEngineAlias");
            p.put(Constants.DATABASE_ZONEID, "UTC");
            mockedUtility.when(() -> Utility.loadProperties("path")).thenReturn(p);
            smsUtilities.when(() -> SmssUtilities.getUniqueName("testEngineAlias", "testEngine"))
                    .thenReturn("testUniqueEngineName");


            sf.when(SecretsFactory::getSecretConnector).thenReturn(null);

            mockedUtility.when(() -> Utility.getDIHelperProperty(Constants.ENCRYPT_SMSS)).thenReturn(null);

            File owlFile = mock(File.class);
            when(owlFile.getAbsolutePath()).thenReturn("path");
            uu.when(() -> UploadUtilities.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, "testEngine", "testEngineAlias"))
                    .thenReturn(owlFile);

            smsUtilities.when(() -> SmssUtilities.getEngineProperties(any(CaseInsensitiveProperties.class)))
                    .thenReturn(null);

            rdfEngineHelper.when(() -> RDFEngineHelper.createBaseFilterHash(rc)).thenReturn(null);

            engine.open("path");

            assertEquals("testEngine", engine.getEngineId());
            assertEquals("testEngineAlias", engine.getEngineName());
            assertEquals(ZoneId.of("UTC"), engine.getDatabaseZoneId());

            assertEquals("path", engine.getOwlFilePath());

            assertEquals(4, engine.getSmssProp().size());

            assertNotNull(engine.getOWLEngineFactory());
            assertNotNull(args.get(0));
            assertEquals(IDatabaseEngine.DATABASE_TYPE.RDBMS, args.get(1));
            assertEquals("testEngine", args.get(2));
            assertEquals("testEngineAlias", args.get(3));

            assertNull(engine.generalEngineProp);
        }
    }

    @Test
    void testOpenSmssPropSecretsMapNull() throws Exception {
        RepositoryConnection rc = mock(RepositoryConnection.class);
        List<Object> args = new ArrayList<>();
        try (MockedStatic<Utility> mockedUtility = Mockito.mockStatic(Utility.class);
             MockedStatic<SmssUtilities> smsUtilities = Mockito.mockStatic(SmssUtilities.class);
             MockedStatic<SecretsFactory> sf = Mockito.mockStatic(SecretsFactory.class);
             MockedStatic<UploadUtilities> uu = Mockito.mockStatic(UploadUtilities.class);
             MockedStatic<RDFEngineHelper> rdfEngineHelper = Mockito.mockStatic(RDFEngineHelper.class);
             MockedConstruction<RDFFileSesameEngine> rdfFileSesameEngine = Mockito.mockConstruction(RDFFileSesameEngine.class, (mock, context) -> {
                 when(mock.getRc()).thenReturn(rc);
             });
             MockedConstruction<OWLEngineFactory> owlEngineFactory = Mockito.mockConstruction(OWLEngineFactory.class, (mock, context) -> {
                 args.add(context.arguments().get(0));
                 args.add(context.arguments().get(1));
                 args.add(context.arguments().get(2));
                 args.add(context.arguments().get(3));
             })) {

            Properties p = new Properties();
            p.put(Constants.ENGINE, "testEngine");
            p.put(Constants.ENGINE_ALIAS, "testEngineAlias");
            p.put(Constants.DATABASE_ZONEID, "UTC");
            mockedUtility.when(() -> Utility.loadProperties("path")).thenReturn(p);
            smsUtilities.when(() -> SmssUtilities.getUniqueName("testEngineAlias", "testEngine"))
                    .thenReturn("testUniqueEngineName");


            ISecrets secrets = mock(ISecrets.class);
            sf.when(SecretsFactory::getSecretConnector).thenReturn(secrets);
            when(secrets.getEngineSecrets(IEngine.CATALOG_TYPE.DATABASE, "testEngine", "testEngineAlias"))
                    .thenReturn(null);

            mockedUtility.when(() -> Utility.getDIHelperProperty(Constants.ENCRYPT_SMSS)).thenReturn(null);

            File owlFile = mock(File.class);
            when(owlFile.getAbsolutePath()).thenReturn("path");
            uu.when(() -> UploadUtilities.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, "testEngine", "testEngineAlias"))
                    .thenReturn(owlFile);

            smsUtilities.when(() -> SmssUtilities.getEngineProperties(any(CaseInsensitiveProperties.class)))
                    .thenReturn(null);

            rdfEngineHelper.when(() -> RDFEngineHelper.createBaseFilterHash(rc)).thenReturn(null);

            engine.open("path");

            assertEquals("testEngine", engine.getEngineId());
            assertEquals("testEngineAlias", engine.getEngineName());
            assertEquals(ZoneId.of("UTC"), engine.getDatabaseZoneId());

            assertEquals("path", engine.getOwlFilePath());

            assertEquals(4, engine.getSmssProp().size());

            assertNotNull(engine.getOWLEngineFactory());
            assertNotNull(args.get(0));
            assertEquals(IDatabaseEngine.DATABASE_TYPE.RDBMS, args.get(1));
            assertEquals("testEngine", args.get(2));
            assertEquals("testEngineAlias", args.get(3));

            assertNull(engine.generalEngineProp);
        }
    }

    @Test
    void testOpenSmssPropSecretsMapEmpty() throws Exception {
        RepositoryConnection rc = mock(RepositoryConnection.class);
        List<Object> args = new ArrayList<>();
        try (MockedStatic<Utility> mockedUtility = Mockito.mockStatic(Utility.class);
             MockedStatic<SmssUtilities> smsUtilities = Mockito.mockStatic(SmssUtilities.class);
             MockedStatic<SecretsFactory> sf = Mockito.mockStatic(SecretsFactory.class);
             MockedStatic<UploadUtilities> uu = Mockito.mockStatic(UploadUtilities.class);
             MockedStatic<RDFEngineHelper> rdfEngineHelper = Mockito.mockStatic(RDFEngineHelper.class);
             MockedConstruction<RDFFileSesameEngine> rdfFileSesameEngine = Mockito.mockConstruction(RDFFileSesameEngine.class, (mock, context) -> {
                 when(mock.getRc()).thenReturn(rc);
             });
             MockedConstruction<OWLEngineFactory> owlEngineFactory = Mockito.mockConstruction(OWLEngineFactory.class, (mock, context) -> {
                 args.add(context.arguments().get(0));
                 args.add(context.arguments().get(1));
                 args.add(context.arguments().get(2));
                 args.add(context.arguments().get(3));
             })) {

            Properties p = new Properties();
            p.put(Constants.ENGINE, "testEngine");
            p.put(Constants.ENGINE_ALIAS, "testEngineAlias");
            p.put(Constants.DATABASE_ZONEID, "UTC");
            mockedUtility.when(() -> Utility.loadProperties("path")).thenReturn(p);
            smsUtilities.when(() -> SmssUtilities.getUniqueName("testEngineAlias", "testEngine"))
                    .thenReturn("testUniqueEngineName");


            ISecrets secrets = mock(ISecrets.class);
            sf.when(SecretsFactory::getSecretConnector).thenReturn(secrets);
            Map<String, Object> secretsMap = new HashMap<>();
            when(secrets.getEngineSecrets(IEngine.CATALOG_TYPE.DATABASE, "testEngine", "testEngineAlias"))
                    .thenReturn(secretsMap);

            mockedUtility.when(() -> Utility.getDIHelperProperty(Constants.ENCRYPT_SMSS)).thenReturn(null);

            File owlFile = mock(File.class);
            when(owlFile.getAbsolutePath()).thenReturn("path");
            uu.when(() -> UploadUtilities.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, "testEngine", "testEngineAlias"))
                    .thenReturn(owlFile);

            smsUtilities.when(() -> SmssUtilities.getEngineProperties(any(CaseInsensitiveProperties.class)))
                    .thenReturn(null);

            rdfEngineHelper.when(() -> RDFEngineHelper.createBaseFilterHash(rc)).thenReturn(null);

            engine.open("path");

            assertEquals("testEngine", engine.getEngineId());
            assertEquals("testEngineAlias", engine.getEngineName());
            assertEquals(ZoneId.of("UTC"), engine.getDatabaseZoneId());

            assertEquals("path", engine.getOwlFilePath());

            assertEquals(4, engine.getSmssProp().size());

            assertNotNull(engine.getOWLEngineFactory());
            assertNotNull(args.get(0));
            assertEquals(IDatabaseEngine.DATABASE_TYPE.RDBMS, args.get(1));
            assertEquals("testEngine", args.get(2));
            assertEquals("testEngineAlias", args.get(3));

            assertNull(engine.generalEngineProp);
        }
    }

    @Test
    void testOpenSmssPropHasSecrets() throws Exception {
        RepositoryConnection rc = mock(RepositoryConnection.class);
        List<Object> args = new ArrayList<>();
        try (MockedStatic<Utility> mockedUtility = Mockito.mockStatic(Utility.class);
             MockedStatic<SmssUtilities> smsUtilities = Mockito.mockStatic(SmssUtilities.class);
             MockedStatic<SecretsFactory> sf = Mockito.mockStatic(SecretsFactory.class);
             MockedStatic<UploadUtilities> uu = Mockito.mockStatic(UploadUtilities.class);
             MockedStatic<RDFEngineHelper> rdfEngineHelper = Mockito.mockStatic(RDFEngineHelper.class);
             MockedConstruction<RDFFileSesameEngine> rdfFileSesameEngine = Mockito.mockConstruction(RDFFileSesameEngine.class, (mock, context) -> {
                 when(mock.getRc()).thenReturn(rc);
             });
             MockedConstruction<OWLEngineFactory> owlEngineFactory = Mockito.mockConstruction(OWLEngineFactory.class, (mock, context) -> {
                 args.add(context.arguments().get(0));
                 args.add(context.arguments().get(1));
                 args.add(context.arguments().get(2));
                 args.add(context.arguments().get(3));
             })) {

            Properties p = new Properties();
            p.put(Constants.ENGINE, "testEngine");
            p.put(Constants.ENGINE_ALIAS, "testEngineAlias");
            p.put(Constants.DATABASE_ZONEID, "UTC");
            mockedUtility.when(() -> Utility.loadProperties("path")).thenReturn(p);
            smsUtilities.when(() -> SmssUtilities.getUniqueName("testEngineAlias", "testEngine"))
                    .thenReturn("testUniqueEngineName");


            ISecrets secrets = mock(ISecrets.class);
            sf.when(SecretsFactory::getSecretConnector).thenReturn(secrets);
            Map<String, Object> secretsMap = new HashMap<>();
            secretsMap.put("s1", "t1");
            secretsMap.put("s2", "t2");
            when(secrets.getEngineSecrets(IEngine.CATALOG_TYPE.DATABASE, "testEngine", "testEngineAlias"))
                    .thenReturn(secretsMap);

            mockedUtility.when(() -> Utility.getDIHelperProperty(Constants.ENCRYPT_SMSS)).thenReturn(null);

            File owlFile = mock(File.class);
            when(owlFile.getAbsolutePath()).thenReturn("path");
            uu.when(() -> UploadUtilities.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, "testEngine", "testEngineAlias"))
                    .thenReturn(owlFile);

            smsUtilities.when(() -> SmssUtilities.getEngineProperties(any(CaseInsensitiveProperties.class)))
                    .thenReturn(null);

            rdfEngineHelper.when(() -> RDFEngineHelper.createBaseFilterHash(rc)).thenReturn(null);

            engine.open("path");

            assertEquals("testEngine", engine.getEngineId());
            assertEquals("testEngineAlias", engine.getEngineName());
            assertEquals(ZoneId.of("UTC"), engine.getDatabaseZoneId());

            assertEquals("path", engine.getOwlFilePath());

            assertEquals("t1", engine.getSmssProp().getProperty("s1"));
            assertEquals("t2", engine.getSmssProp().getProperty("s2"));
            assertEquals(6, engine.getSmssProp().size());

            assertNotNull(engine.getOWLEngineFactory());
            assertNotNull(args.get(0));
            assertEquals(IDatabaseEngine.DATABASE_TYPE.RDBMS, args.get(1));
            assertEquals("testEngine", args.get(2));
            assertEquals("testEngineAlias", args.get(3));

            assertNull(engine.generalEngineProp);
        }
    }

    ///
    /// setDatabaseZoneId
    ///

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"NEVERLAND"})
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
   @ValueSource(strings = { "UTC", "America/Chicago"})
   void testDbZoneID(String src) {
       CaseInsensitiveProperties p = new CaseInsensitiveProperties();
       p.put(Constants.DATABASE_ZONEID, src);
       engine.setSmssProp(p);
       engine.setDatabaseZoneId();
       assertEquals(ZoneId.of(src), engine.getDatabaseZoneId());
   }


    ///
    /// Simple methods
    ///

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
}
