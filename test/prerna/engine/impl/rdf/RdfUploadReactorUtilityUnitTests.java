package prerna.engine.impl.rdf;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDFDatabase;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;

public class RdfUploadReactorUtilityUnitTests {

    // Have a good start here. Need to get API tests in for this to understand real data

    private RDFFileSesameEngine setupRdfFileSesameEngine(Path tempDir) throws Exception {
        RDFFileSesameEngine engine = new RDFFileSesameEngine();
        Path rdf = tempDir.resolve("rdf.owl");
        Files.createDirectories(rdf.getParent());
        URI uri = rdf.toUri();
        String baseUri = uri.toString();
        String rdfPath = rdf.toAbsolutePath().toString();
        URL url = RdfUploadReactorUtility.class.getResource("movie-book-title.owl");
        Path p = Paths.get(url.toURI());
        Files.copy(p, rdf);

        String engineId = "engine-01";
        String engineAlias = "ea";

        Properties props = new Properties();
        props.setProperty(Constants.ENGINE, engineId);
        props.setProperty(Constants.ENGINE_ALIAS, engineAlias);
        props.setProperty(Constants.RDF_FILE_NAME, rdfPath);
        props.setProperty(Constants.RDF_FILE_PATH, rdfPath);
        props.setProperty(Constants.RDF_FILE_BASE_URI, baseUri);
        props.setProperty(Constants.RDF_FILE_TYPE, "RDF/XML");

        String typeQuery = "SELECT ?entity WHERE {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <@entity@>;}";
        props.setProperty(Constants.TYPE_QUERY, typeQuery);

        engine.setBasic(true);

        String engineNameAndId = SmssUtilities.getUniqueName(engineAlias, engineId);
        Path engineFolder = tempDir.resolve(Constants.VECTOR_FOLDER).resolve(engineNameAndId);
        Path engineAssetFolder = engineFolder.resolve("assets");
        Path engineVersionFolder = engineFolder.resolve("version");

        try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
            DIHelper diMock = mock(DIHelper.class);
            dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
            when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
            try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class)) {
                eu.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
                        .thenReturn(engineFolder.toString());
                eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
                        .thenReturn(engineAssetFolder.toString());
                eu.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.VECTOR, engineNameAndId))
                        .thenReturn(engineVersionFolder.toString());

                engine.open(props);
            }
        }

        return engine;
    }

    private WriteOWLEngine setupWriteOwlEngine(Path tempDir) throws Exception {
        Semaphore semaphore = new Semaphore(0);
        RDFFileSesameEngine rdfFileSesameEngine = setupRdfFileSesameEngine(tempDir);

        WriteOWLEngine woe = new WriteOWLEngine(semaphore,
                rdfFileSesameEngine,
                IDatabaseEngine.DATABASE_TYPE.SESAME,
                "engine-01",
                "ea"
        );
        return woe;
    }

    private IRDFDatabase setupDatabaseEngine() throws RepositoryException {
        InMemorySesameEngine engine = new InMemorySesameEngine();
        Repository myRepository = new SailRepository(
                new ForwardChainingRDFSInferencer(
                        new MemoryStore()));
        myRepository.initialize();
        engine.setRepositoryConnection(myRepository.getConnection());
        return engine;
    }

    @Test
    void testLoadMetadataIntoEngine(@TempDir Path tempDir) throws Exception {
        IRDFDatabase engine = setupDatabaseEngine();
        try (WriteOWLEngine woe = setupWriteOwlEngine(tempDir)) {
            RdfUploadReactorUtility.loadMetadataIntoEngine(engine, woe);
        }
        String query = "ASK WHERE { \n" +
                "<http://semoss.org/ontologies/Relation/Contains/BOOK/TITLE> ?p ?o .\n" +
                "}";
        Boolean bool = (Boolean) engine.execQuery(query);
        assertTrue(bool);
    }

    @Test
    void testCreateRelationship(@TempDir Path tempDir) throws Exception {
        IRDFDatabase engine = setupDatabaseEngine();
        try (WriteOWLEngine woe = setupWriteOwlEngine(tempDir)) {
            RdfUploadReactorUtility.loadMetadataIntoEngine(engine, woe);
        }
        String query = "ASK WHERE { \n" +
                "<http://semoss.org/ontologies/Relation/Contains/BOOK/TITLE> ?p ?o .\n" +
                "}";
        Boolean bool = (Boolean) engine.execQuery(query);
        assertTrue(bool);
    }
}
