package prerna.engine.impl.rdf;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import prerna.engine.api.IDatabaseEngine;
import prerna.util.Constants;

import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SesameJenaBooleanWrapperUnitTests {

    private SesameJenaBooleanWrapper wrapper;
    private IDatabaseEngine engine;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws Exception {
        wrapper = new SesameJenaBooleanWrapper();

        engine = new RDFFileSesameEngine();
        Path rdf = tempDir.resolve("rdf.owl");
        Files.createDirectories(rdf.getParent());
        URI uri = rdf.toUri();
        URL url = RdfUploadReactorUtility.class.getResource("movie-book-title.owl");
        String rdfPath = rdf.toAbsolutePath().toString();
        assert url != null;
        Path p = Paths.get(url.toURI());
        Files.copy(p, rdf);

        Path smss = tempDir.resolve("engine-01.smss");
        Files.createFile(smss);

        Properties props = new Properties();
        props.setProperty(Constants.ENGINE, "engine-01");
        props.setProperty(Constants.ENGINE_ALIAS, "ea");
        props.setProperty(Constants.RDF_FILE_NAME, rdfPath);
        props.setProperty(Constants.RDF_FILE_PATH, rdfPath);
        props.setProperty(Constants.RDF_FILE_BASE_URI, uri.toString());
        props.setProperty(Constants.RDF_FILE_TYPE, "RDF/XML");

        String typeQuery = "SELECT ?entity WHERE {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <@entity@>;}";
        props.setProperty(Constants.TYPE_QUERY, typeQuery);

        engine.setBasic(true);
        engine.open(props);

        wrapper.setEngine(engine);
    }

    @Test
    void testSesameJenaBooleanWrapper() throws Exception {
        String query = "ASK WHERE { \n" +
                "<http://semoss.org/ontologies/Relation/Contains/BOOK/TITLE> ?p ?o .\n" +
                "}";

        wrapper.setQuery(query);

        assertTrue(wrapper.execute());
    }
}
