package prerna.engine.impl.rdf;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.impl.InfModelImpl;
import org.apache.jena.update.UpdateAction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import prerna.engine.api.IDatabaseEngine;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class InMemoryJenaEngineUnitTests {

    private InMemoryJenaEngine engine;

    @Mock
    private Model jenaModel;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        engine = new InMemoryJenaEngine();
        engine.jenaModel = jenaModel;
    }

    @Test
    void testExecQuery() {
        String queryString = "query";
        Query query = mock(Query.class);
        QueryExecution qex = mock(QueryExecution.class);
        ResultSet resultSet = mock(ResultSet.class);

        try (MockedStatic<QueryFactory> qf = Mockito.mockStatic(QueryFactory.class);
             MockedStatic<QueryExecutionFactory> qef = Mockito.mockStatic(QueryExecutionFactory.class);) {

            qf.when(() -> QueryFactory.create(queryString)).thenReturn(query);
            qef.when(() -> QueryExecutionFactory.create(query, jenaModel)).thenReturn(qex);
            when(qex.execSelect()).thenReturn(resultSet);

            ResultSet results = (ResultSet) engine.execQuery("query");
            assertEquals(resultSet, results);
        }
    }


    @Test
    void testExecQueryErrorReturnsNull() {
        String queryString = "query";
        Query query = mock(Query.class);
        QueryExecution qex = mock(QueryExecution.class);

        try (MockedStatic<QueryFactory> qf = Mockito.mockStatic(QueryFactory.class);
             MockedStatic<QueryExecutionFactory> qef = Mockito.mockStatic(QueryExecutionFactory.class);) {

            qf.when(() -> QueryFactory.create(queryString)).thenReturn(query);
            qef.when(() -> QueryExecutionFactory.create(query, jenaModel)).thenReturn(qex);

            RuntimeException e = new RuntimeException("test");
            when(qex.execSelect()).thenThrow(e);

            ResultSet results = (ResultSet) engine.execQuery("query");
            assertNull(results);
        }
    }

    @Test
    void testInsertData() {
        String queryString = "query";
        try (MockedStatic<UpdateAction> updateActionMock = Mockito.mockStatic(UpdateAction.class)) {
            engine.insertData(queryString);
            updateActionMock.verify(() -> UpdateAction.parseExecute(queryString, jenaModel), times(1));
        }
    }

    @Test
    void testSetModel() {
        Model jena2 = mock(Model.class);
        engine.setModel(jena2);
        assertEquals(jena2, engine.jenaModel);
    }

    @Test
    void testGetDatabaseType() {
        assertEquals(IDatabaseEngine.DATABASE_TYPE.JENA, engine.getDatabaseType());
    }

    @Test
    void testIsConnected() {
        assertFalse(engine.isConnected());
    }

    @Test
    void testHoldsFileLocks() {
        assertFalse(engine.holdsFileLocks());
    }
}
