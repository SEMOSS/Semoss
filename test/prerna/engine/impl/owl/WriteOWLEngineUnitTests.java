package prerna.engine.impl.owl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Hashtable;
import java.util.Properties;
import java.util.concurrent.Semaphore;

import static org.junit.jupiter.api.Assertions.*;

public class WriteOWLEngineUnitTests {

    private WriteOWLEngine engine = null;

    private Semaphore semaphore = null;

    private RDFFileSesameEngine rfse = null;

    private Path rdf = null;

    @BeforeEach
    void setup(@TempDir Path tempDir) throws Exception {
        Properties coreProp = new Properties();
        coreProp.setProperty(Constants.BASE_FOLDER, tempDir.toString());
        DIHelper.getInstance().setCoreProp(coreProp);

        semaphore = new Semaphore(1);

        rdf = tempDir.resolve("rdf.owl");
        Files.createDirectories(rdf.getParent());

        URL url = WriteOWLEngineUnitTests.class.getResource("movie-book.owl");
        assert url != null;

        URI uri = rdf.toUri();
        String baseUri = uri.toString();
        String rdfPath = rdf.toAbsolutePath().toString();

        Path p = Paths.get(url.toURI());
        Files.copy(p, rdf);

        Path smss = tempDir.resolve("engine-01.smss");
        Files.createFile(smss);


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

        rfse = new RDFFileSesameEngine();
        rfse.setBasic(true);
        rfse.open(props);
        rfse.createBaseRelationEngine();

        Path db = tempDir.resolve("db");
        Path ea = db.resolve("ea__engine-01");
        Files.createDirectories(ea);

        Files.createFile(ea.resolve("ea_OWL.OWL"));

        engine = new WriteOWLEngine(semaphore, rfse, IDatabaseEngine.DATABASE_TYPE.SESAME, "engine-01", "ea");
    }

    @Test
    void testClose() throws IOException {
        assertEquals(1, semaphore.availablePermits());
        engine.close();
        assertEquals(2, semaphore.availablePermits());
    }

    @Test
    void loadDatabaseValues() {
        engine.loadDatabaseValues();

        Hashtable<String, String> concept = engine.getConceptHash();
        Hashtable<String, String> prop = engine.getPropHash();
        Hashtable<String, String> relation = engine.getRelationHash();

        assertEquals(1, concept.size());
        assertEquals(1, prop.size());
        // Unsure on how to get relations in the hash
        assertEquals(0, relation.size());

        assertEquals("http://semoss.org/ontologies/Concept/TITLE", concept.get("TITLE"));
        assertEquals("http://semoss.org/ontologies/Relation/Contains/BOOK/TITLE", prop.get("TITLE%TITLE"));
    }

    @Test
    void testCreateEmptyOWLFile() throws Exception {
        engine.createEmptyOWLFile();

        Hashtable<String, String> concept = engine.getConceptHash();
        Hashtable<String, String> prop = engine.getPropHash();
        Hashtable<String, String> relation = engine.getRelationHash();

        assertEquals(0, concept.size());
        assertEquals(0, prop.size());
        assertEquals(0, relation.size());
    }

    @Test
    void testReloadFile() throws Exception {
        URL url = WriteOWLEngineUnitTests.class.getResource("empty.owl");
        assert url != null;
        Path p = Paths.get(url.toURI());
        Files.copy(p, rdf, StandardCopyOption.REPLACE_EXISTING);

        engine.reloadOWLFile();

        engine.loadDatabaseValues();
        Hashtable<String, String> concept = engine.getConceptHash();
        Hashtable<String, String> prop = engine.getPropHash();
        Hashtable<String, String> relation = engine.getRelationHash();

        assertEquals(0, concept.size());
        assertEquals(0, prop.size());
        assertEquals(0, relation.size());
    }

    @Test
    void testAddConcept() {
        String val = engine.addConcept("Author");
        assertEquals("http://semoss.org/ontologies/Concept/Author", val);

        Hashtable<String, String> concept = engine.getConceptHash();
        assertEquals(2, concept.size());
        assertEquals("http://semoss.org/ontologies/Concept/TITLE", concept.get("TITLE"));
        assertEquals("http://semoss.org/ontologies/Concept/Author", concept.get("Author"));
    }

    @Test
    void testAddRelation() throws IOException {
        engine.addConcept("AUTHOR");
        engine.addProp("AUTHOR", "MOVIE", "STRING");
        engine.addRelation("AUTHOR", "TITLE", "MOVIE");

        Hashtable<String, String> relations = engine.getRelationHash();
        assertEquals(1, relations.size());
        assertEquals("http://semoss.org/ontologies/Relation/MOVIE", relations.get("AUTHORTITLEMOVIE"));

        engine.export(false);

        String fileContents = new String(Files.readAllBytes(rdf), StandardCharsets.UTF_8);

        String val = "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/AUTHOR\">\n" +
                "\t<DatatypeProperty xmlns=\"http://www.w3.org/2002/07/owl#\" rdf:resource=\"http://semoss.org/ontologies/Relation/Contains/MOVIE\"/>\n" +
                "</rdf:Description>\n" +
                "\n" +
                "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Relation/Contains/MOVIE\">\n" +
                "\t<Class rdf:resource=\"TYPE:STRING\"/>\n" +
                "\t<Pixel xmlns=\"http://semoss.org/ontologies/Relation/\" rdf:resource=\"http://semoss.org/ontologies/Relation/Contains/MOVIE/AUTHOR\"/>\n" +
                "\t<Conceptual xmlns=\"http://semoss.org/ontologies/Relation/\">MOVIE</Conceptual>\n" +
                "</rdf:Description>\n" +
                "\n" +
                "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Relation/MOVIE\">\n" +
                "\t<subPropertyOf rdf:resource=\"http://semoss.org/ontologies/Relation\"/>\n" +
                "</rdf:Description>\n" +
                "\n" +
                "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/AUTHOR\">\n" +
                "\t<MOVIE xmlns=\"http://semoss.org/ontologies/Relation/\" rdf:resource=\"http://semoss.org/ontologies/Concept/TITLE\"/>\n" +
                "</rdf:Description>";

        assertEquals(1, fileContents.split(val).length - 1);
    }

    @Test
    void testAddProp() {
        engine.addProp("TITLE", "Author", "STRING");
        Hashtable<String, String> prop = engine.getPropHash();
        assertEquals(2, prop.size());
        assertEquals("http://semoss.org/ontologies/Relation/Contains/BOOK/TITLE", prop.get("TITLE%TITLE"));
        assertEquals("http://semoss.org/ontologies/Relation/Contains/Author", prop.get("TITLE%Author"));
    }

    //@Test
    // Need a bigger fix where we either can reset EngineUtility base folder
    // or we create a parent test class that can then use the same temp directory
    // Second approach would be nice because then we can create utils to create
    // engines for testing faster.
    void testAddUniqueCounts() throws IOException {

        Semaphore s1 = new Semaphore(1);
        WriteOWLEngine writer = new WriteOWLEngine(s1, rfse.getBaseDataEngine(), IDatabaseEngine.DATABASE_TYPE.SESAME,
                "engine-01", "ea");
        writer.addConcept("TITLE");
        writer.addProp("TITLE", "MOVIE", "STRING");
        writer.addProp("TITLE", "BOOK", "STRING");

        writer.export(false);

        engine.addUniqueCounts(rfse);

        String fileContents = new String(Files.readAllBytes(rdf), StandardCharsets.UTF_8);

        String unique1 = "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/TITLE\">\n" +
                "\t<UNIQUE xmlns=\"http://semoss.org/ontologies/Relation/Contains/\">0</UNIQUE>\n" +
                "</rdf:Description>\n" +
                "\n" +
                "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Relation/Contains/MOVIE\">\n" +
                "\t<UNIQUE xmlns=\"http://semoss.org/ontologies/Relation/Contains/\">0</UNIQUE>\n" +
                "</rdf:Description>\n" +
                "\n" +
                "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Relation/Contains/BOOK\">\n" +
                "\t<UNIQUE xmlns=\"http://semoss.org/ontologies/Relation/Contains/\">0</UNIQUE>\n" +
                "</rdf:Description>";

        // assert this string only occurs once
        assertEquals(1, fileContents.split(unique1).length - 1);
    }

    @Test
    void addSubClass() throws IOException {
        engine.addSubclass("CHILD", "PARENT");
        Hashtable<String, String> concepts = engine.getConceptHash();
        assertEquals(3, concepts.size());
        assertEquals("http://semoss.org/ontologies/Concept/PARENT", concepts.get("PARENT"));
        assertEquals("http://semoss.org/ontologies/Concept/CHILD", concepts.get("CHILD"));

        engine.export(false);

        String fileContents = new String(Files.readAllBytes(rdf), StandardCharsets.UTF_8);

        String val = "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/CHILD\">\n" +
                "\t<subClassOf rdf:resource=\"http://semoss.org/ontologies/Concept\"/>\n" +
                "\t<Class rdf:resource=\"TYPE:STRING\"/>\n" +
                "\t<Pixel xmlns=\"http://semoss.org/ontologies/Relation/\" rdf:resource=\"http://semoss.org/ontologies/Concept/CHILD\"/>\n" +
                "\t<Conceptual xmlns=\"http://semoss.org/ontologies/Relation/\">CHILD</Conceptual>\n" +
                "</rdf:Description>\n" +
                "\n" +
                "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/PARENT\">\n" +
                "\t<subClassOf rdf:resource=\"http://semoss.org/ontologies/Concept\"/>\n" +
                "\t<Class rdf:resource=\"TYPE:STRING\"/>\n" +
                "\t<Pixel xmlns=\"http://semoss.org/ontologies/Relation/\" rdf:resource=\"http://semoss.org/ontologies/Concept/PARENT\"/>\n" +
                "\t<Conceptual xmlns=\"http://semoss.org/ontologies/Relation/\">PARENT</Conceptual>\n" +
                "</rdf:Description>\n" +
                "\n" +
                "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/CHILD\">\n" +
                "\t<subClassOf rdf:resource=\"http://semoss.org/ontologies/Concept/PARENT\"/>\n" +
                "</rdf:Description>";

        assertEquals(1, fileContents.split(val).length - 1);
    }

    @Test
    void testRemoveConcept() throws IOException {
        NounMetadata nm = engine.removeConcept("TITLE");

        assertTrue((Boolean) nm.getValue());
        assertEquals("Successfully removed concept and all its dependencies",
                nm.getAdditionalReturn().get(0).getValue().toString());

        // I'm not a huge fan of exporting and then reading the file
        // but it works and makes testing easier.
        engine.export(false);

        String fileContents = new String(Files.readAllBytes(rdf), StandardCharsets.UTF_8);

        String concept = "rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/TITLE\">\n" +
                "\t<subClassOf rdf:resource=\"http://semoss.org/ontologies/Concept\"/>\n" +
                "\t<domain>noData</domain>\n" +
                "\t<Pixel xmlns=\"http://semoss.org/ontologies/Relation/\" rdf:resource=\"http://semoss.org/ontologies/Concept/TITLE\"/>\n" +
                "\t<Conceptual xmlns=\"http://semoss.org/ontologies/Relation/\">TITLE</Conceptual>\n" +
                "</rdf:Description>";

        String movieDataType = "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/TITLE\">\n" +
                "\t<DatatypeProperty xmlns=\"http://www.w3.org/2002/07/owl#\" rdf:resource=\"http://semoss.org/ontologies/Relation/Contains/MOVIE/TITLE\"/>\n" +
                "</rdf:Description>";

        String bookDataType = "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/TITLE\">\n" +
                "\t<DatatypeProperty xmlns=\"http://www.w3.org/2002/07/owl#\" rdf:resource=\"http://semoss.org/ontologies/Relation/Contains/BOOK/TITLE\"/>\n" +
                "</rdf:Description>";

        assertFalse(fileContents.contains(concept));
        assertFalse(fileContents.contains(movieDataType));
        assertFalse(fileContents.contains(bookDataType));

        Hashtable<String, String> concepts = engine.getConceptHash();
        assertFalse(concepts.containsKey("TITLE"));
    }

    @Test
    void testRemoveRelation() throws IOException {
        // setup engine
        testAddRelation();

        // remove relation
        engine.removeRelation("AUTHOR", "TITLE", "MOVIE");

        Hashtable<String, String> relations = engine.getRelationHash();
        assertEquals(0, relations.size());

        engine.export(false);

        String fileContents = new String(Files.readAllBytes(rdf), StandardCharsets.UTF_8);

        String val = "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/AUTHOR\">\n" +
                "\t<DatatypeProperty xmlns=\"http://www.w3.org/2002/07/owl#\" rdf:resource=\"http://semoss.org/ontologies/Relation/Contains/MOVIE\"/>\n" +
                "</rdf:Description>\n" +
                "\n" +
                "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Relation/Contains/MOVIE\">\n" +
                "\t<Class rdf:resource=\"TYPE:STRING\"/>\n" +
                "\t<Pixel xmlns=\"http://semoss.org/ontologies/Relation/\" rdf:resource=\"http://semoss.org/ontologies/Relation/Contains/MOVIE/AUTHOR\"/>\n" +
                "\t<Conceptual xmlns=\"http://semoss.org/ontologies/Relation/\">MOVIE</Conceptual>\n" +
                "</rdf:Description>\n" +
                "\n" +
                "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Relation/MOVIE\">\n" +
                "\t<subPropertyOf rdf:resource=\"http://semoss.org/ontologies/Relation\"/>\n" +
                "</rdf:Description>\n" +
                "\n" +
                "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/AUTHOR\">\n" +
                "\t<MOVIE xmlns=\"http://semoss.org/ontologies/Relation/\" rdf:resource=\"http://semoss.org/ontologies/Concept/TITLE\"/>\n" +
                "</rdf:Description>";

        assertFalse(fileContents.contains(val));
    }

    @Test
    void testRemoveProp() {
        NounMetadata nm = engine.removeProp("TITLE", "TITLE");
        assertTrue((Boolean) nm.getValue());
        assertEquals("Successfully removed property", nm.getAdditionalReturn().get(0).getValue().toString());

        assertEquals(0, engine.getPropHash().size());
    }

    @Test
    void testRenameConcept() throws IOException {
        engine.addConcept("OLD");
        engine.export(false);

        String original = new String(Files.readAllBytes(rdf), StandardCharsets.UTF_8);
        int originalCount = original.split("OLD").length - 1;

        NounMetadata nm = engine.renameConcept("OLD", "NEW", "NEW");
        assertTrue((Boolean) nm.getValue());
        assertEquals("Successfully removed concept and all its dependencies", nm.getAdditionalReturn().get(0).getValue()
                .toString());

        engine.export(false);

        String fileContents = new String(Files.readAllBytes(rdf), StandardCharsets.UTF_8);
        int newCount = fileContents.split("NEW").length - 1;

        // make sure same amount of keywords and length of files are equal
        assertEquals(originalCount, newCount);

        // Should the concept hash get updated?
        // assertEquals("", engine.getConceptHash().get("NEW"));
    }

    @Test
    void testRenameProp() throws IOException {
        engine.addConcept("CONCEPT");
        engine.addProp("CONCEPT", "OLD", "STRING");
        engine.export(false);

        String original = new String(Files.readAllBytes(rdf), StandardCharsets.UTF_8);
        int originalCount = original.split("OLD").length - 1;

        NounMetadata nm = engine.renameProp("CONCEPT", "OLD", "NEW");
        assertTrue((Boolean) nm.getValue());
        // Should change the error message
        assertEquals("Successfully removed property", nm.getAdditionalReturn().get(0).getValue()
                .toString());

        engine.export(false);

        String fileContents = new String(Files.readAllBytes(rdf), StandardCharsets.UTF_8);
        int newCount = fileContents.split("NEW").length - 1;

        assertEquals(5, originalCount);
        assertEquals(2, newCount);

        // Should the concept hash get updated?
        assertEquals("http://semoss.org/ontologies/Relation/Contains/OLD", engine.getPropHash().get("CONCEPT%OLD"));
    }

}
