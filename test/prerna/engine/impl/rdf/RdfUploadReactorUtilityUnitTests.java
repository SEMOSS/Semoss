package prerna.engine.impl.rdf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.Constants;

import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Semaphore;

public class RdfUploadReactorUtilityUnitTests {

    private RDFFileSesameEngine setupRdfFileSesameEngine(Path tempDir) throws Exception {
        RDFFileSesameEngine engine = new RDFFileSesameEngine();
        Path rdf = tempDir.resolve("rdf.owl");
        Files.createDirectories(rdf.getParent());
        URI uri = rdf.toUri();
        String baseUri = uri.toString();
        String rdfPath = rdf.toAbsolutePath().toString();
        RdfUploadReactorUtility.class.g
        URL url = RdfUploadReactorUtility.class.getResource("movie-book-title.owl");
        Path p = Paths.get(url.toURI());
        Files.move(p, rdf);

        Properties props = new Properties();
        props.setProperty(Constants.ENGINE, "engine-01");
        props.setProperty(Constants.ENGINE_ALIAS, "ea");
        props.setProperty(Constants.RDF_FILE_NAME, rdfPath);
        props.setProperty(Constants.RDF_FILE_PATH, rdfPath);
        props.setProperty(Constants.RDF_FILE_BASE_URI, baseUri);
        props.setProperty(Constants.RDF_FILE_TYPE, "RDF/XML");

        // not exactly sure what to put here
        String typeQuery = "";
        props.setProperty(Constants.TYPE_QUERY, "");

        engine.setBasic(true);

        engine.open(props);

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

    private IDatabaseEngine setupDatabaseEngine() throws RepositoryException {
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
        try (WriteOWLEngine woe = setupWriteOwlEngine(tempDir)) {
            IDatabaseEngine engine = setupDatabaseEngine();
            RdfUploadReactorUtility.loadMetadataIntoEngine(engine, woe);
        }
    }
}
