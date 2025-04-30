package prerna.engine.impl.rdbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.engine.api.IDatabaseEngine;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.ImpalaSqlInterpreter;

public class ImpalaEngineUnitTests {

    private ImpalaEngine impalaEngine;

    @BeforeEach
    void setUp() {
        impalaEngine = new ImpalaEngine();
    }

    @Test
    void testGetQueryInterpreter() {
        IQueryInterpreter queryInterpreter = impalaEngine.getQueryInterpreter();
        assertNotNull(queryInterpreter);
        assertEquals(ImpalaSqlInterpreter.class, queryInterpreter.getClass());
    }

    @Test
    void testGetDatabaseType() {
        IDatabaseEngine.DATABASE_TYPE databaseType = impalaEngine.getDatabaseType();
        assertNotNull(databaseType);
        assertEquals(IDatabaseEngine.DATABASE_TYPE.IMPALA, databaseType);
    }
}
