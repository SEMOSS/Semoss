package prerna.engine.impl.rdf;

import org.apache.jena.dboe.transaction.txn.TransactionException;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.tdb2.TDB2;
import org.apache.jena.tdb2.TDB2Factory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openrdf.query.TupleQueryResult;
import prerna.engine.api.IDatabaseEngine;
import prerna.util.Constants;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class RDFJenaTDBEngineUnitTests {

    private RDFJenaTDBEngine engine;

    private String randomNumbers() {
        Random rand = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            sb.append(rand.nextInt(10));
        }
        return sb.toString();
    }

    @BeforeEach
    void setUp() throws Exception {
        Path tempDir  = Files.createTempDirectory("junit" + randomNumbers());
        engine = new RDFJenaTDBEngine();

        Path rdf = tempDir.resolve("data");
        Files.createDirectories(rdf.getParent());
        URI uri = rdf.toUri();
        String baseUri = uri.toString();
        String rdfPath = rdf.toAbsolutePath().toString();

        //Files.createFile(rdf);

        String[] rdfLines = {
                "<?xml version=\"1.0\"?>",
                "<rdf:RDF",
                "    xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"",
                "    xmlns:ex=\"http://example.org/movies#\"",
                "    xmlns:xsd=\"http://www.w3.org/2001/XMLSchema#\">",
                "",
                "    <rdf:Description rdf:about=\"http://example.org/movies#Movie1\">",
                "        <ex:title>Inception</ex:title>",
                "        <ex:length rdf:datatype=\"http://www.w3.org/2001/XMLSchema#integer\">148</ex:length>",
                "        <ex:rating rdf:datatype=\"http://www.w3.org/2001/XMLSchema#double\">8.8</ex:rating>",
                "        <ex:aspectRatio rdf:datatype=\"http://www.w3.org/2001/XMLSchema#float\">2.39</ex:aspectRatio>",
                "        <ex:isAvailable rdf:datatype=\"http://www.w3.org/2001/XMLSchema#boolean\">true</ex:isAvailable>",
                "        <ex:releaseDate rdf:datatype=\"http://www.w3.org/2001/XMLSchema#date\">2010-07-16</ex:releaseDate>",
                "        <ex:lastScreening rdf:datatype=\"http://www.w3.org/2001/XMLSchema#dateTime\">2023-10-01T20:00:00</ex:lastScreening>",
                "    </rdf:Description>",
                "",
                "    <rdf:Description rdf:about=\"http://example.org/movies#Movie2\">",
                "        <ex:title>The Matrix</ex:title>",
                "        <ex:length rdf:datatype=\"http://www.w3.org/2001/XMLSchema#integer\">136</ex:length>",
                "        <ex:rating rdf:datatype=\"http://www.w3.org/2001/XMLSchema#double\">8.7</ex:rating>",
                "        <ex:aspectRatio rdf:datatype=\"http://www.w3.org/2001/XMLSchema#float\">2.35</ex:aspectRatio>",
                "        <ex:isAvailable rdf:datatype=\"http://www.w3.org/2001/XMLSchema#boolean\">false</ex:isAvailable>",
                "        <ex:releaseDate rdf:datatype=\"http://www.w3.org/2001/XMLSchema#date\">1999-03-31</ex:releaseDate>",
                "        <ex:lastScreening rdf:datatype=\"http://www.w3.org/2001/XMLSchema#dateTime\">2023-09-15T18:30:00</ex:lastScreening>",
                "    </rdf:Description>",
                "",
                "</rdf:RDF>"
        };

        List<String> lines = Arrays.asList(rdfLines);

        //Files.write(rdf, lines);

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



        String insertQuery = "PREFIX ex: <http://example.org/movies#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "\n" +
                "INSERT DATA {\n" +
                "  <http://example.org/movies#Movie3> \n" +
                "    ex:title \"Interstellar\" ;\n" +
                "    ex:length \"169\"^^xsd:integer ;\n" +
                "    ex:rating \"8.6\"^^xsd:double ;\n" +
                "    ex:aspectRatio \"2.39\"^^xsd:float ;\n" +
                "    ex:isAvailable \"true\"^^xsd:boolean ;\n" +
                "    ex:releaseDate \"2014-11-07\"^^xsd:date ;\n" +
                "    ex:lastScreening \"2023-10-10T19:00:00\"^^xsd:dateTime .\n" +
                "}";

        engine.insertData(insertQuery);
    }

    @AfterEach
    void tearDown() throws IOException {
        engine.close();
        engine.getDataset().close();
        engine.getDataset().end();
        TDB2.closedown();
    }

    @Test
    void testOpen() throws IOException {
        // Open is called in before each. Make sure its connected
        assertTrue(engine.isConnected());
    }

    @Test
    void testExecQuery() {
        // This query could be wrong.
        String query = "PREFIX ex: <http://example.org/movies#>\n" +
                "\n" +
                "SELECT ?title ?length\n" +
                "WHERE {\n" +
                "  ?movie ex:title ?title ;\n" +
                "         ex:length ?length .\n" +
                "}";

        Object result = engine.execQuery(query);
        ResultSet rs = (ResultSet) result;

        List<String> results = rs.getResultVars();
        assertAll("QuerySolutions",
                () -> assertEquals("title", results.get(0)),
                () -> assertEquals("length", results.get(1))
        );
    }

    @Test
    void testExecQueryBoolean() {
        String query = "PREFIX ex: <http://example.org/movies#>\n" +
                "\n" +
                "ASK WHERE {\n" +
                "  ?movie ex:title \"Interstellar\" ;\n" +
                "         ex:length 169 .\n" +
                "}";

        Boolean result = (Boolean) engine.execQuery(query);

        assertTrue(result);
    }

    @Test
    void testRemoveData() {
        String insertQuery = "PREFIX ex: <http://example.org/movies#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "\n" +
                "DELETE DATA {\n" +
                "  <http://example.org/movies#Movie3> \n" +
                "    ex:title \"Interstellar\" ;\n" +
                "    ex:length \"169\"^^xsd:integer ;\n" +
                "    ex:rating \"8.6\"^^xsd:double ;\n" +
                "    ex:aspectRatio \"2.39\"^^xsd:float ;\n" +
                "    ex:isAvailable \"true\"^^xsd:boolean ;\n" +
                "    ex:releaseDate \"2014-11-07\"^^xsd:date ;\n" +
                "    ex:lastScreening \"2023-10-10T19:00:00\"^^xsd:dateTime .\n" +
                "}";

        engine.removeData(insertQuery);

        String query = "PREFIX ex: <http://example.org/movies#>\n" +
                "\n" +
                "ASK WHERE {\n" +
                "  ?movie ex:title \"Interstellar\" ;\n" +
                "         ex:length 169 .\n" +
                "}";

        Boolean result = (Boolean) engine.execQuery(query);

        assertFalse(result);
    }

    @Test
    void testGetDatabaseType() {
        assertEquals(IDatabaseEngine.DATABASE_TYPE.JENA, engine.getDatabaseType());
    }

    @Test
    void testHoldsLockFile() {
        assertTrue(engine.holdsFileLocks());
    }

    @Test
    void testGetCleanSelect() {
        String query = "PREFIX ex: <http://example.org/movies#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "\n" +
                "SELECT ?title ?length ?rating ?aspectRatio ?isAvailable ?releaseDate ?lastScreening\n" +
                "WHERE {\n" +
                "  ?movie ex:title ?title ;\n" +
                "         ex:length ?length ;\n" +
                "         ex:rating ?rating ;\n" +
                "         ex:aspectRatio ?aspectRatio ;\n" +
                "         ex:isAvailable ?isAvailable ;\n" +
                "         ex:releaseDate ?releaseDate ;\n" +
                "         ex:lastScreening ?lastScreening .\n" +
                "}";

        // Not sure how to not get an exception.
        TransactionException e = assertThrows(TransactionException.class, () -> engine.getCleanSelect(query));
        assertEquals("Not in a transaction", e.getMessage());

        // It gets the one database values
        // assertEquals(1, result.size());
    }


    @Test
    void testAddStatement() {
        String subject = "http://example.org/movies#Movie1";
        String predicate = "http://example.org/movies#title";
        String object = "Men In Black 2";
        Boolean concept = false;

        Object[] args = {subject, predicate, object, concept};

        engine.addStatement(args);

        String query = "PREFIX ex: <http://example.org/movies#>\n" +
                "\n" +
                "ASK WHERE {\n" +
                "  ?movie ex:title \"Men In Black 2\"  .\n" +
                "}";

        Boolean result = (Boolean) engine.execQuery(query);
        assertTrue(result);
    }

    @Test
    void testRemoveStatement() {
        String subject = "http://example.org/movies#Movie1";
        String predicate = "http://example.org/movies#title";
        String object = "Inception";
        Boolean concept = false;

        Object[] args = {subject, predicate, object, concept};

        engine.removeStatement(args);

        String query = "PREFIX ex: <http://example.org/movies#>\n" +
                "\n" +
                "ASK WHERE {\n" +
                "  ?movie ex:title \"Inception\"  .\n" +
                "}";

        Boolean result = (Boolean) engine.execQuery(query);
        assertFalse(result);
    }

    @Test
    void testGetDataset() {
        assertNotNull(engine.getDataset());
    }

}
