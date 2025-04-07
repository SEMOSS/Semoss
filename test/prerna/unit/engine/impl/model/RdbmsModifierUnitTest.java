package prerna.unit.engine.impl.model;



import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.sql.SQLException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import prerna.engine.impl.model.AbstractModelEngine;
import prerna.engine.impl.modifications.EngineModificationFactory;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.modifications.RdbmsModifier;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.DATABASE_TYPE;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngineModifier;
import prerna.util.sql.AbstractSqlQueryUtil;

public class RdbmsModifierUnitTest {
    private RDBMSNativeEngine mockEngine;
    private RdbmsModifier modifier; 
    private AbstractSqlQueryUtil mockQueryUtil;
    private String table = "users";
    private String column = "income";
    private String dataType = "INT";
    private String sqlQuery = "ALTER TABLE users ADD COLUMN income INT";

    @BeforeEach
    void setUp() {
        mockEngine = mock(RDBMSNativeEngine.class);
        
        mockQueryUtil = mock(AbstractSqlQueryUtil.class);

        when(mockEngine.getQueryUtil()).thenReturn(mockQueryUtil);
        
        when(mockEngine.getDatabase()).thenReturn("test_db");
        
        when(mockEngine.getSchema()).thenReturn("public");

        modifier = new RdbmsModifier();
        modifier.setEngine(mockEngine);
    } 

    @Test
    void testAddPropertyAllowColumnSucc() throws Exception {
        when(mockQueryUtil.allowAddColumn()).thenReturn(true);
        when(mockQueryUtil.alterTableAddColumn(table, column, dataType)).thenReturn(sqlQuery);

        modifier.addProperty(table, column, dataType);
        verify(mockEngine).insertData(sqlQuery);
    }

    @Test
    void testAddPropertyAllowColumnException() throws Exception {
        when(mockQueryUtil.allowAddColumn()).thenReturn(true);
        when(mockQueryUtil.alterTableAddColumn(table, column, dataType)).thenReturn(sqlQuery);
        doThrow(new SQLException("SQL error")).when(mockEngine).insertData(sqlQuery);

        try{
            modifier.addProperty(table, column, dataType);
        } catch (SQLException e) {
            // assmuing i need to test for the exact error message or maybe something else?
            assert(e.getMessage().contains("Error occurred to alter the table. See logs for details."));
        }
        // do I need to verify this if im just testing for the exception
        verify(mockEngine).insertData(sqlQuery);
    }

    @Test
    void testAddPropertyAllowColumnFalse() throws Exception{
        when(mockQueryUtil.allowAddColumn()).thenReturn(false);
        modifier.addProperty(table, column, sqlQuery);
        verify(mockEngine, never()).insertData(anyString());
    }

    //@Test
    // void testGetEngineModifierRdbms() {
    //     try(MockedConstruction<RdbmsModifier> rmc = Mockito.mockConstruction(RdbmsModifier.class, (mock, context) -> {
    //         //do nothing
    //     })) {
    //         when(dbEngine.getDatabaseType()).thenReturn(DATABASE_TYPE.RDBMS);
    //         IEngineModifier testing = EngineModificationFactory.getEngineModifier(dbEngine); // class.method for every test for static calls
    //         assertNotNull(testing);
    //         RdbmsModifier red = (RdbmsModifier) testing;
    //         assertNotNull(red);
    //     }

    // }

}