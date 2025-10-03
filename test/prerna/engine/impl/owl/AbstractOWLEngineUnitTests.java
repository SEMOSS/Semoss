package prerna.engine.impl.owl;

import org.apache.jena.vocabulary.OWL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Utility;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class AbstractOWLEngineUnitTests {

    private AbstractOWLEngine engine = null;

    @Mock
    private RDFFileSesameEngine bde;

    @Mock
    private RepositoryConnection rc;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);

        engine = new AbstractOWLEngine(bde, "eid", "ename") {
            @Override
            public RDFFileSesameEngine getBaseDataEngine() {
                return bde;
            }

            @Override
            public void setBaseDataEngine(RDFFileSesameEngine baseDataEngine) {
                this.baseDataEngine = baseDataEngine;
            }
        };

        when(bde.getRc()).thenReturn(rc);
    }

    @Test
    void testQuery() throws Exception {
        try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {
            WrapperManager instance = mock(WrapperManager.class);
            wm.when(() -> WrapperManager.getInstance()).thenReturn(instance);
            IRawSelectWrapper irsw = mock(IRawSelectWrapper.class);
            when(instance.getRawWrapper(bde, "query")).thenReturn(irsw);

            IRawSelectWrapper result = engine.query("query");
            assertEquals(irsw, result);
        }
    }

    @Test
    void testGetFromNeighbors() {
        try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
            Vector<String> neighbors = new Vector<>();
            neighbors.add("testone");
            util.when(() -> Utility.getVectorOfReturn("SELECT DISTINCT ?node WHERE { BIND(<physical> AS ?start) {?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?node ?rel ?start}}",
                    bde, true)).thenReturn(neighbors);

            Vector<String> result = engine.getFromNeighbors("physical", 1);
            assertEquals("testone", result.get(0));
        }
    }

    @Test
    void getToNeighbors() {
        try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
            Vector<String> neighbors = new Vector<>();
            neighbors.add("testone");

            util.when(() -> Utility.getVectorOfReturn("SELECT DISTINCT ?node WHERE { BIND(<physical> AS ?start) {?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?start ?rel ?node}}",
                    bde, true)).thenReturn(neighbors);

            Vector<String> result = engine.getToNeighbors("physical", 1);
            assertEquals("testone", result.get(0));
        }
    }

    @Test
    void getNeighbors() {
        try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
            Vector<String> neighbors = new Vector<>();
            neighbors.add("testone");

            Vector<String> toNeighbors = new Vector<>();
            toNeighbors.add("testtwo");

            util.when(() -> Utility.getVectorOfReturn("SELECT DISTINCT ?node WHERE { BIND(<physical> AS ?start) {?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?node ?rel ?start}}",
                    bde, true)).thenReturn(neighbors);

            util.when(() -> Utility.getVectorOfReturn("SELECT DISTINCT ?node WHERE { BIND(<physical> AS ?start) {?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?start ?rel ?node}}",
                    bde, true)).thenReturn(toNeighbors);


            Vector<String> result = engine.getNeighbors("physical", 1);
            assertEquals("testone", result.get(0));
            assertEquals("testtwo", result.get(1));
        }
    }

    @Test
    void testGetOwlDef() throws RDFHandlerException, RepositoryException {
        String owlDef = engine.getOWLDefinition();

        assertEquals("", owlDef);

        verify(rc, times(1)).export(any(RDFXMLWriter.class));
    }


    @Test
    void testGetOwlFilePath() {
        assertNull(engine.getOwlFilePath());
    }

    @Test
    void testGetProperty() {
        assertNull(engine.getProperty("key"));
    }


    @Test
    void testExecuteInsightQuery() {
        assertNull(engine.executeInsightQuery("test", false));
    }

    @Test
    void testGetNodeBaseURI() throws Exception {
        try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {
            WrapperManager instance = mock(WrapperManager.class);
            wm.when(() -> WrapperManager.getInstance()).thenReturn(instance);
            IRawSelectWrapper irsw = mock(IRawSelectWrapper.class);
            when(instance.getRawWrapper(bde, "SELECT DISTINCT ?entity WHERE { { <SEMOSS:ENGINE_METADATA> <CONTAINS:BASE_URI> ?entity } } LIMIT 1"))
                    .thenReturn(irsw);

            when(irsw.hasNext()).thenReturn(true);
            IHeadersDataRow row = mock(IHeadersDataRow.class);
            when(row.getRawValues()).thenReturn(new Object[] {"test"});
            when(irsw.next()).thenReturn(row);
            String result = engine.getNodeBaseUri();
            assertEquals("test", result);
        }
    }

    @Test
    void testGetNodeBaseURINull() throws Exception {
        try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {
            WrapperManager instance = mock(WrapperManager.class);
            wm.when(() -> WrapperManager.getInstance()).thenReturn(instance);
            IRawSelectWrapper irsw = mock(IRawSelectWrapper.class);
            when(instance.getRawWrapper(bde, "SELECT DISTINCT ?entity WHERE { { <SEMOSS:ENGINE_METADATA> <CONTAINS:BASE_URI> ?entity } } LIMIT 1"))
                    .thenReturn(irsw);

            when(irsw.hasNext()).thenReturn(false);
            String result = engine.getNodeBaseUri();
            assertEquals("http://semoss.org/ontologies/Concept/", result);
        }
    }

    @Test
    void testGetConcepts() {
        try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
            Vector<String> neighbors = new Vector<>();
            neighbors.add("testone");

            String query = "SELECT ?concept WHERE { {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
                    + " Filter(?concept != <http://semoss.org/ontologies/Concept>) }";

            util.when(() -> Utility.getVectorOfReturn(query, bde, true)).thenReturn(neighbors);


            Vector<String> result = engine.getConcepts();
            assertEquals("testone", result.get(0));
        }
    }

    @Test
    void testExecOntoSelectQuery() {
        String query = "foo";
        String val = "bar";

        when(bde.execQuery(query)).thenReturn(val);

        assertEquals(val, engine.execOntoSelectQuery(query));
    }

    @Test
    void testOntoInsertData() {
        String query = "foo";

        engine.ontoInsertData(query);

        verify(bde, times(1)).insertData(query);
    }

    @Test
    void testOntoRemoveData() {
        String query = "foo";

        engine.ontoRemoveData(query);

        verify(bde, times(1)).removeData(query);
    }

    @Test
    void testGetDataTypes() throws Exception {
        String uri = "uri";
        String query = "SELECT DISTINCT ?TYPE WHERE { {<" + uri + "> <" + RDFS.CLASS.toString() + "> ?TYPE} }";

        try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {
            WrapperManager instance = mock(WrapperManager.class);
            wm.when(() -> WrapperManager.getInstance()).thenReturn(instance);
            IRawSelectWrapper irsw = mock(IRawSelectWrapper.class);
            when(instance.getRawWrapper(bde, query)).thenReturn(irsw);

            when(irsw.hasNext()).thenReturn(true);
            IHeadersDataRow row = mock(IHeadersDataRow.class);
            when(row.getValues()).thenReturn(new Object[] {"test"});
            when(irsw.next()).thenReturn(row);

            String result = engine.getDataTypes(uri);

            assertEquals("test", result);
            verify(irsw, times(1)).close();
        }
    }

    @Test
    void testGetDataTypesUris() throws Exception {
        String query = "SELECT DISTINCT ?NODE ?TYPE WHERE { {?NODE <http://www.w3.org/2000/01/rdf-schema#Class> ?TYPE} }" +
                " BINDINGS ?NODE {(<uri>)(<uri1>)}";

        try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {
            WrapperManager instance = mock(WrapperManager.class);
            wm.when(() -> WrapperManager.getInstance()).thenReturn(instance);
            IRawSelectWrapper irsw = mock(IRawSelectWrapper.class);
            when(instance.getRawWrapper(bde, query)).thenReturn(irsw);

            when(irsw.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
            IHeadersDataRow row = mock(IHeadersDataRow.class);
            when(row.getValues()).thenReturn(new Object[] {"foo", "bar"}).thenReturn(new Object[] {"foo1", "bar1"});
            when(irsw.next()).thenReturn(row);

            String[] uris = {"uri", "uri1"};
            Map<String, String> result = engine.getDataTypes(uris);

            assertEquals("bar", result.get("foo"));
            assertEquals("bar1", result.get("foo1"));
            verify(irsw, times(1)).close();
        }
    }

    @Test
    void testGetAdtlDataTypes() throws Exception {
        String query = "SELECT DISTINCT ?ADTLTYPE WHERE { {<uri> <http://semoss.org/ontologies/Relation/AdtlDataType> ?ADTLTYPE} }";

        try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {
            WrapperManager instance = mock(WrapperManager.class);
            wm.when(() -> WrapperManager.getInstance()).thenReturn(instance);
            IRawSelectWrapper irsw = mock(IRawSelectWrapper.class);
            when(instance.getRawWrapper(bde, query)).thenReturn(irsw);

            when(irsw.hasNext()).thenReturn(true).thenReturn(false);
            IHeadersDataRow row = mock(IHeadersDataRow.class);
            when(row.getValues()).thenReturn(new Object[] {"ADTLTYPE:bar"});
            when(irsw.next()).thenReturn(row);

            String result = engine.getAdtlDataTypes("uri");

            assertEquals("bar", result);
            verify(irsw, times(1)).close();
        }
    }

    @Test
    void testGetAdtlDataTypeUris() throws Exception {
        String query = "SELECT DISTINCT ?NODE ?ADTLTYPE WHERE { {?NODE <http://semoss.org/ontologies/Relation/AdtlDataType> ?ADTLTYPE} } BINDINGS ?NODE {(<uri>)(<uri1>)}";

        try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {
            WrapperManager instance = mock(WrapperManager.class);
            wm.when(() -> WrapperManager.getInstance()).thenReturn(instance);
            IRawSelectWrapper irsw = mock(IRawSelectWrapper.class);
            when(instance.getRawWrapper(bde, query)).thenReturn(irsw);

            when(irsw.hasNext()).thenReturn(true).thenReturn(false);
            IHeadersDataRow row = mock(IHeadersDataRow.class);
            when(row.getValues()).thenReturn(new Object[] {"foo", "ADTLTYPE:bar"});
            when(irsw.next()).thenReturn(row);

            String[] uris = {"uri", "uri1"};
            Map<String, String> result = engine.getAdtlDataTypes(uris);

            assertEquals("bar", result.get("foo"));
            verify(irsw, times(1)).close();
        }
    }

    @Test
    void testGetMetamodel() throws Exception {
        String query = "SELECT DISTINCT ?concept ?property WHERE { {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }OPTIONAL {{?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> } {?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property } }}";
        String query2 = "SELECT DISTINCT ?fromConceptualConcept ?rel ?toConceptualConcept WHERE { {?fromConcept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?toConcept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?fromConcept ?rel ?toConcept} {?fromConcept <http://semoss.org/ontologies/Relation/Conceptual> ?fromConceptualConcept }{?toConcept <http://semoss.org/ontologies/Relation/Conceptual> ?toConceptualConcept }}";

        try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class);
            MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {

            WrapperManager instance = mock(WrapperManager.class);
            wm.when(() -> WrapperManager.getInstance()).thenReturn(instance);
            IRawSelectWrapper irsw = mock(IRawSelectWrapper.class);
            when(instance.getRawWrapper(bde, query)).thenReturn(irsw);


            when(irsw.hasNext()).thenReturn(true).thenReturn(false);
            IHeadersDataRow row = mock(IHeadersDataRow.class);
            when(row.getRawValues()).thenReturn(new Object[] {"foo", "prop"});
            when(irsw.next()).thenReturn(row);

            IRawSelectWrapper irsw2 = mock(IRawSelectWrapper.class);
            when(irsw2.hasNext()).thenReturn(true).thenReturn(false);
            IHeadersDataRow row2 = mock(IHeadersDataRow.class);
            when(irsw2.next()).thenReturn(row2);
            when(row2.getValues()).thenReturn(new Object[] {"from", "rel", "to"});
            when(row2.getRawValues()).thenReturn(new Object[] {"from", "rel", "to"});

            when(instance.getRawWrapper(bde, query2)).thenReturn(irsw2);

            util.when(() -> Utility.getInstanceName("foo")).thenReturn("bar");
            util.when(() -> Utility.getClassName("prop")).thenReturn("java.util.String");

            Map<String, Object[]> result = engine.getMetamodel();

            Object[] nodes = result.get("nodes");
            Object[] edges = result.get("edges");

            assertNotNull(nodes[0]);
            assertNotNull(edges[0]);
            verify(irsw, times(1)).close();
            verify(irsw2, times(1)).close();
        }
    }

    @Test
    void testGetPositionFile() {
        assertNull(engine.getOwlPositionFile());
    }

    @Test
    void testGetQueryInterpreter() {
        assertNull(engine.getQueryInterpreter());
    }

    @Test
    void testIsBasic() {
        assertTrue(engine.isBasic());
    }

    @Test
    void testSetBasic() {
        engine.setBasic(false);
        assertTrue(engine.isBasic());
    }

    @Test
    void testGetPixelConcepts() {
        String query = "SELECT ?pixelName WHERE {"
                + " {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
                + " {?concept <http://semoss.org/ontologies/Relation/Pixel> ?pixelName }"
                + " }";
        try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
            Vector<String> list = new Vector<>();
            list.add("list");

            util.when(() -> Utility.getVectorOfReturn(query, bde, false)).thenReturn(list);

            List<String> result = engine.getPixelConcepts();

            assertEquals("list", result.get(0));
        }
    }

    @Test
    void testGetPixelSelectors() {
        String cpi = "cpi";
        String query = "SELECT DISTINCT ?pixelName WHERE { "
                + " BIND(<http://semoss.org/ontologies/Concept/" + cpi + "> as ?concept) "
                + " {?concept <http://semoss.org/ontologies/Relation/Pixel> ?pixelName }"
                + " FILTER NOT EXISTS {?concept <http://www.w3.org/2000/01/rdf-schema#domain> \"noData\" }"
                + " }";

        String query2 = "SELECT DISTINCT ?pixelName WHERE {  BIND(<http://semoss.org/ontologies/Concept/cpi> as ?concept)  {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }  {?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>}  {?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property}  {?property <http://semoss.org/ontologies/Relation/Pixel> ?pixelName} }";

        try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
            Vector<String> vector = new Vector<>();
            vector.add("test");
            Vector<String> vector2 = new Vector<>();
            vector2.add("test");
            util.when(() -> Utility.getVectorOfReturn(query, bde, false)).thenReturn(vector);
            util.when(() -> Utility.getVectorOfReturn(query2, bde, true)).thenReturn(vector2);
            util.when(() -> Utility.getClassName("test")).thenReturn("testName");

            List<String> result = engine.getPixelSelectors(cpi);
            assertEquals("test", result.get(0));
            assertEquals("cpi__testName", result.get(1));
        }
    }

    @Test
    void testGetPropertyPixelSelectors() {
        String cpi = "cpi";
        String query = "SELECT DISTINCT ?pixelName WHERE {  BIND(<http://semoss.org/ontologies/Concept/cpi> as ?concept)  {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }  {?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>}  {?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property}  {?property <http://semoss.org/ontologies/Relation/Pixel> ?pixelName} }";

        try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
            Vector<String> vector = new Vector<>();
            vector.add("test");
            util.when(() -> Utility.getVectorOfReturn(query, bde, false)).thenReturn(vector);

            List<String> result = engine.getPropertyPixelSelectors(cpi);
            assertEquals("cpi__test", result.get(0));
        }
    }

    @Test
    void testPhysicalConcpets() {
        String query = "SELECT ?concept WHERE {"
                + "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
                + "Filter(?concept != <http://semoss.org/ontologies/Concept>)"
                + "}";

        try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
            Vector<String> vector = new Vector<>();
            vector.add("test");
            util.when(() -> Utility.getVectorOfReturn(query, bde, true)).thenReturn(vector);

            List<String> result = engine.getPhysicalConcepts();
            assertEquals("test", result.get(0));
        }
    }

    @Test
    void testGetPhysicalRelationships() {
        String query = "SELECT DISTINCT ?start ?end ?rel WHERE { "
                + "{?start <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
                + "{?end <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
                + "{?rel <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>} "
                + "{?start ?rel ?end}"
                + "Filter(?rel != <" + RDFS.SUBPROPERTYOF + ">)"
                + "Filter(?rel != <http://semoss.org/ontologies/Relation>)"
                + "}";

        try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
            Vector<String[]> vector = new Vector<>();
            vector.add(new String[] {"test"});
            util.when(() -> Utility.getVectorArrayOfReturn(query, bde, true)).thenReturn(vector);

            List<String[]> result = engine.getPhysicalRelationships();
            assertEquals("test", result.get(0)[0]);
        }
    }

    @Test
    void testGetPropertyUris4PhysicalUri() {
        String physicalUri = "uri";
        String query = "SELECT DISTINCT ?property WHERE { "
                + "BIND(<" + physicalUri + "> AS ?concept) "
                + "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> } "
//					+ "{?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} "
                + "{?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property} "
                + "}";

        try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
            Vector<String> vector = new Vector<>();
            vector.add("test");
            util.when(() -> Utility.getVectorOfReturn(query, bde, true)).thenReturn(vector);

            List<String> result = engine.getPropertyUris4PhysicalUri(physicalUri);
            assertEquals("test", result.get(0));
        }
    }

    @Test
    void testGetPhysicalUriFromPixelSelectors() {
        String pixelSelector = "foo__bar";
        String query = "SELECT DISTINCT ?property WHERE {  BIND(<http://semoss.org/ontologies/Relation/Contains/bar/foo> as ?pixelName)  {?property <http://semoss.org/ontologies/Relation/Pixel> ?pixelName }  }";

        try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
            Vector<String> vector = new Vector<>();
            vector.add("test");
            util.when(() -> Utility.getVectorOfReturn(query, bde, true)).thenReturn(vector);

            String result = engine.getPhysicalUriFromPixelSelector(pixelSelector);
            assertEquals("test", result);
        }
    }


    @Test
    void testGetConceptPixelUriFromPhysicalUri() {
        String physicalUri = "uri";

        String query = "SELECT DISTINCT ?pixel WHERE {  BIND(<uri> as ?physicalUri)  {?physicalUri <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> } {?physicalUri <http://semoss.org/ontologies/Relation/Pixel> ?pixel }  }";

        try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
            Vector<String> vector = new Vector<>();
            vector.add("test");
            util.when(() -> Utility.getVectorOfReturn(query, bde, true)).thenReturn(vector);

            String result = engine.getConceptPixelUriFromPhysicalUri(physicalUri);
            assertEquals("test", result);
        }
    }

    @Test
    void testGetPropertyPixelUriFromPhysicalUri() throws Exception {
        String concept = "concept";
        String physical = "physical";

        String query = "SELECT DISTINCT ?pixel ?parentPixel WHERE { "
                + " BIND(<" + physical + "> as ?propertyPhysicalUri) "
                + " BIND(<" + concept + "> as ?conceptPhysicalUri) "
                + " {?conceptPhysicalUri <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
                + " {?conceptPhysicalUri <http://semoss.org/ontologies/Relation/Pixel> ?parentPixel } "
                + " {?propertyPhysicalUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} "
                + "	{?conceptPhysicalUri <http://www.w3.org/2002/07/owl#DatatypeProperty> ?propertyPhysicalUri} "
                + " {?propertyPhysicalUri <http://semoss.org/ontologies/Relation/Pixel> ?pixel } "
                + " }";

        try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class);
             MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {

            WrapperManager instance = mock(WrapperManager.class);
            wm.when(() -> WrapperManager.getInstance()).thenReturn(instance);
            IRawSelectWrapper irsw = mock(IRawSelectWrapper.class);
            when(instance.getRawWrapper(bde, query)).thenReturn(irsw);

            when(irsw.hasNext()).thenReturn(true).thenReturn(false);
            IHeadersDataRow row = mock(IHeadersDataRow.class);
            when(row.getRawValues()).thenReturn(new Object[] {"prop", "parent"});
            when(irsw.next()).thenReturn(row);

            util.when(() -> Utility.getInstanceName("parent")).thenReturn("foo");
            util.when(() -> Utility.getInstanceName("prop")).thenReturn("foo");

            String result = engine.getPropertyPixelUriFromPhysicalUri(concept, physical);

            assertEquals("prop", result);

            verify(irsw, times(1)).close();
        }

    }

    @Test
    void testGetPixelSelectorsFromPhysicalUri() throws Exception {
        String physicalUri = "uri";
        String query = "SELECT DISTINCT ?pixel ?type WHERE { "
                + " {"
                + " BIND(\"concept\" as ?type) "
                + " BIND(<" + physicalUri + "> as ?physicalUri) "
                + " {?physicalUri <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
                + " {?physicalUri <http://semoss.org/ontologies/Relation/Pixel> ?pixel } "
                + " }"
                + " UNION "
                + "	{"
                + " BIND(\"property\" as ?type) "
                + " BIND(<" + physicalUri + "> as ?physicalUri) "
                + " {?physicalUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> }"
                + " {?physicalUri <http://semoss.org/ontologies/Relation/Pixel> ?pixel } "
                + "	}"
                + " }";

        try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class);
             MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {

            WrapperManager instance = mock(WrapperManager.class);
            wm.when(WrapperManager::getInstance).thenReturn(instance);
            IRawSelectWrapper irsw = mock(IRawSelectWrapper.class);
            when(instance.getRawWrapper(bde, query)).thenReturn(irsw);

            when(irsw.hasNext()).thenReturn(true).thenReturn(false);
            IHeadersDataRow row = mock(IHeadersDataRow.class);
            when(row.getRawValues()).thenReturn(new Object[] {"prop", "concept"});
            when(irsw.next()).thenReturn(row);

            util.when(() -> Utility.getInstanceName("prop")).thenReturn("foo");

            String result = engine.getPixelSelectorFromPhysicalUri(physicalUri);

            assertEquals("foo", result);

            verify(irsw, times(1)).close();
        }
    }

    @Test
    void testGetConceptualNames() throws Exception {
        String physicalUri = "uri";
        String query = "SELECT DISTINCT ?conceptual WHERE { "
                + "BIND(<" + physicalUri + "> AS ?uri) "
                + "{?uri <" + "http://semoss.org/ontologies/Relation/Conceptual" + "> ?conceptual } "
                + "}";

        try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {

            WrapperManager instance = mock(WrapperManager.class);
            wm.when(WrapperManager::getInstance).thenReturn(instance);
            IRawSelectWrapper irsw = mock(IRawSelectWrapper.class);
            when(instance.getRawWrapper(bde, query)).thenReturn(irsw);

            when(irsw.hasNext()).thenReturn(true).thenReturn(false);
            IHeadersDataRow row = mock(IHeadersDataRow.class);
            when(row.getValues()).thenReturn(new Object[] {"concept", "second"});
            when(irsw.next()).thenReturn(row);

            String result = engine.getConceptualName(physicalUri);

            assertEquals("concept", result);

            verify(irsw, times(1)).close();
        }
    }

    @Test
    void getLogicalNames() throws Exception {
        String physicalUri = "uri";
        String query = "SELECT DISTINCT ?logical WHERE { "
                + "BIND(<" + physicalUri + "> AS ?uri) "
                + "{?uri <" + OWL.sameAs.toString() + "> ?logical } "
                + "}";

        try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {

            WrapperManager instance = mock(WrapperManager.class);
            wm.when(WrapperManager::getInstance).thenReturn(instance);
            IRawSelectWrapper irsw = mock(IRawSelectWrapper.class);
            when(instance.getRawWrapper(bde, query)).thenReturn(irsw);

            when(irsw.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
            IHeadersDataRow row = mock(IHeadersDataRow.class);
            when(row.getValues()).thenReturn(new Object[] {"val"}).thenReturn(new Object[]{"val2"});
            when(irsw.next()).thenReturn(row).thenReturn(row);

            Set<String> result = engine.getLogicalNames(physicalUri);

            assertTrue(result.contains("val"));
            assertTrue(result.contains("val2"));

            verify(irsw, times(1)).close();
        }
    }

    @Test
    void testGetDescription() throws Exception {
        String physicalUri = "uri";
        String query = "SELECT DISTINCT ?description WHERE { "
                + "BIND(<" + physicalUri + "> AS ?uri) "
                + "{?uri <" + RDFS.COMMENT.toString() + "> ?description } "
                + "}";

        try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {

            WrapperManager instance = mock(WrapperManager.class);
            wm.when(WrapperManager::getInstance).thenReturn(instance);
            IRawSelectWrapper irsw = mock(IRawSelectWrapper.class);
            when(instance.getRawWrapper(bde, query)).thenReturn(irsw);

            when(irsw.hasNext()).thenReturn(true).thenReturn(false);
            IHeadersDataRow row = mock(IHeadersDataRow.class);
            when(row.getValues()).thenReturn(new Object[] {"concept", "second"});
            when(irsw.next()).thenReturn(row);

            String result = engine.getDescription(physicalUri);

            assertEquals("concept", result);

            verify(irsw, times(1)).close();
        }
    }

}
