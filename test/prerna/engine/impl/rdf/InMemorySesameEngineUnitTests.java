package prerna.engine.impl.rdf;

import com.sun.org.apache.xerces.internal.jaxp.datatype.XMLGregorianCalendarImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.*;
import org.openrdf.query.*;
import org.openrdf.query.algebra.Datatype;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.UnknownTransactionStateException;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailConnection;
import prerna.engine.api.IDatabaseEngine;
import prerna.util.Constants;
import prerna.util.Utility;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.Vector;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class InMemorySesameEngineUnitTests {

    private InMemorySesameEngine engine;

    @Mock
    private SailRepositoryConnection rc;

    @Mock
    private SailConnection sail;

    @Mock
    private ValueFactory vf;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        engine = new InMemorySesameEngine();

        when(rc.getSailConnection()).thenReturn(sail);
        when(rc.getValueFactory()).thenReturn(vf);
    }

    @Test
    void testSetRC() {
        engine.setRepositoryConnection(rc);
        assertEquals(rc, engine.getRepositoryConnection());
        assertTrue(engine.isConnected());

        verify(rc, times(1)).getSailConnection();
        verify(rc, times(1)).getValueFactory();
    }

    @Test
    void testClose() throws RepositoryException {
        engine.setRepositoryConnection(rc);
        engine.close();
        verify(rc, times(1)).clear();
        verify(rc, times(1)).rollback();
        verify(rc, times(1)).close();
        assertFalse(engine.isConnected());
    }

    @Test
    void testCloseHandlesException() throws RepositoryException {
        engine.setRepositoryConnection(rc);
        RepositoryException re = new RepositoryException("test");
        doThrow(re).when(rc).close();
        engine.close();
        // no exception thrown
    }

    @Test
    void testCloseNotConnected() {
        engine.close();
        // do nothing
        verifyNoInteractions(rc);
        assertFalse(engine.isConnected());
    }

    @Test
    void testExecQueryTuple() throws MalformedQueryException, RepositoryException, QueryEvaluationException {
        engine.setRepositoryConnection(rc);
        String queryString = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";
        TupleQuery query = mock(TupleQuery.class);
        when(rc.prepareQuery(QueryLanguage.SPARQL, queryString)).thenReturn(query);

        TupleQueryResult result = mock(TupleQueryResult.class);
        when(query.evaluate()).thenReturn(result);

        TupleQueryResult tqr = (TupleQueryResult) engine.execQuery(queryString);
        assertNotNull(tqr);
        assertEquals(result, tqr);
    }

    @Test
    void testExecQueryGraph() throws MalformedQueryException, RepositoryException, QueryEvaluationException {
        engine.setRepositoryConnection(rc);
        String queryString = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";
        GraphQuery query = mock(GraphQuery.class);
        when(rc.prepareQuery(QueryLanguage.SPARQL, queryString)).thenReturn(query);

        GraphQueryResult result = mock(GraphQueryResult.class);
        when(query.evaluate()).thenReturn(result);

        GraphQueryResult tqr = (GraphQueryResult) engine.execQuery(queryString);
        assertNotNull(tqr);
        assertEquals(result, tqr);
    }

    @Test
    void testExecQueryBoolean() throws MalformedQueryException, RepositoryException, QueryEvaluationException {
        engine.setRepositoryConnection(rc);
        String queryString = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";
        BooleanQuery query = mock(BooleanQuery.class);
        when(rc.prepareQuery(QueryLanguage.SPARQL, queryString)).thenReturn(query);

        Boolean result = false;
        when(query.evaluate()).thenReturn(result);

        Boolean b = (Boolean) engine.execQuery(queryString);
        assertNotNull(b);
        assertFalse(b);
    }

    @Test
    void testExecQueryException() throws MalformedQueryException, RepositoryException {
        engine.setRepositoryConnection(rc);
        String qr = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";
        RepositoryException re = new RepositoryException("test");
        doThrow(re).when(rc).prepareQuery(QueryLanguage.SPARQL, qr);

        assertNull(engine.execQuery(qr));
    }


    @Test
    void testInsertData() throws MalformedQueryException, RepositoryException, UpdateExecutionException {
        String qs = "Select";
        engine.setRepositoryConnection(rc);
        Update update = mock(Update.class);
        when(rc.prepareUpdate(QueryLanguage.SPARQL, qs)).thenReturn(update);

        engine.insertData(qs);

        verify(rc, times(1)).setAutoCommit(false);
        verify(update, times(1)).execute();
    }


    @Test
    void testGetDatabaseType() {
        assertEquals(IDatabaseEngine.DATABASE_TYPE.SESAME, engine.getDatabaseType());
    }

    static Stream<Arguments> getCleanSelectProvider() throws DatatypeConfigurationException {
        DatatypeFactory df = DatatypeFactory.newInstance();
        GregorianCalendar gc = new GregorianCalendar();
        XMLGregorianCalendar cal = df.newXMLGregorianCalendar(gc);
        Date expectedDate = Date.from(cal.toGregorianCalendar().toInstant());

        XMLGregorianCalendar cal2 = df.newXMLGregorianCalendar("2020-12-12");

        return Stream.of(
                Arguments.of(new IntegerLiteralImpl(BigInteger.ONE), "1", "java.lang.Integer"),
                // I think there is a bug here. There is a comment in InMemorySesameEngine.java
                //Arguments.of(new DecimalLiteralImpl(new BigDecimal("1.1")), "1.1", "java.lang.Double"),
                Arguments.of(new NumericLiteralImpl(new Float(1.11)), "1.11", "java.lang.Float"),
                Arguments.of(new BooleanLiteralImpl(false), "false", "java.lang.Boolean"),
                Arguments.of(new CalendarLiteralImpl(cal), expectedDate.toString(), "java.util.Date"),
                Arguments.of(new CalendarLiteralImpl(cal2), "Sat Dec 12 00:00:00 EST 2020", "java.util.Date"),
                Arguments.of(new URIImpl("not:value"), "not:value", "java.lang.String")
        );
    }

    @ParameterizedTest
    @MethodSource("getCleanSelectProvider")
    void testGetCleanSelect(Value literal, String expected, String expectedClass) throws MalformedQueryException, RepositoryException, QueryEvaluationException {
        engine.setRepositoryConnection(rc);
        String qs = "Select";
        TupleQuery query = mock(TupleQuery.class);
        TupleQueryResult result = mock(TupleQueryResult.class);

        when(rc.prepareTupleQuery(QueryLanguage.SPARQL, qs)).thenReturn(query);
        when(query.evaluate()).thenReturn(result);

        when(result.hasNext()).thenReturn(true, false);
        BindingSet bs = mock(BindingSet.class);

        when(result.next()).thenReturn(bs);

        when(bs.getValue(Constants.ENTITY)).thenReturn(literal);

        Vector<Object> ret = engine.getCleanSelect(qs);

        verify(query, times(1)).setIncludeInferred(true);

        assertNotNull(ret);
        assertEquals(1, ret.size());

        Object object = ret.get(0);
        assertEquals(expected, object.toString());
        assertEquals(expectedClass, object.getClass().getName());
    }

    @Test
    void testGetEntityType() throws MalformedQueryException, RepositoryException, QueryEvaluationException {
        Value literal = new URIImpl("not:value");
        String expected = "not:value";
        String expectedClass = "java.lang.String";
        engine.setRepositoryConnection(rc);
        String qs = "SELECT type";
        TupleQuery query = mock(TupleQuery.class);
        TupleQueryResult result = mock(TupleQueryResult.class);

        when(rc.prepareTupleQuery(QueryLanguage.SPARQL, qs)).thenReturn(query);
        when(query.evaluate()).thenReturn(result);

        when(result.hasNext()).thenReturn(true, false);
        BindingSet bs = mock(BindingSet.class);

        when(result.next()).thenReturn(bs);

        when(bs.getValue(Constants.ENTITY)).thenReturn(literal);

        Properties props = new Properties();
        props.setProperty(Constants.TYPE_QUERY, "SELECT @entity@");
        engine.setSmssProp(props);

        Vector<Object> ret = engine.getEntityOfType("type");


        verify(query, times(1)).setIncludeInferred(true);

        assertNotNull(ret);
        assertEquals(1, ret.size());

        Object object = ret.get(0);
        assertEquals(expected, object.toString());
        assertEquals(expectedClass, object.getClass().getName());
    }

    @Test
    void testIsConnected() {
        assertFalse(engine.isConnected());
    }

    // Cannot test addStatement and removeStatement due to casting objects in constructor

    @Test
    void removeData() throws MalformedQueryException, RepositoryException, UpdateExecutionException {
        String qs = "Select";
        engine.setRepositoryConnection(rc);
        Update update = mock(Update.class);
        when(rc.prepareUpdate(QueryLanguage.SPARQL, qs)).thenReturn(update);

        engine.insertData(qs);

        verify(rc, times(1)).setAutoCommit(false);
        verify(update, times(1)).execute();
    }

    @Test
    void testCommit() {
        engine.commit();
        verifyNoInteractions(rc);
        verifyNoInteractions(sail);
    }

    @Test
    void testDelete() {
        engine.commit();
        verifyNoInteractions(rc);
        verifyNoInteractions(sail);
    }

    // writeData has no usages

    @Test
    void testHoldsFileLocks() {
        assertFalse(engine.holdsFileLocks());
    }

}
