package prerna.engine.impl.modifications;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.sql.AbstractSqlQueryUtil;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class RDBMSModifierUnitTests {

    private RdbmsModifier modifier;

    @Mock
    private RDBMSNativeEngine engine;

    @Mock
    private AbstractSqlQueryUtil queryUtil;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        modifier = new RdbmsModifier();

        when(engine.getQueryUtil()).thenReturn(queryUtil);
        when(engine.getDatabase()).thenReturn("db");
        when(engine.getSchema()).thenReturn("schema");

        modifier.setEngine(engine);
    }

    @Test
    void testAddProperty() throws Exception {
        when(queryUtil.allowAddColumn()).thenReturn(true);
        when(queryUtil.alterTableAddColumn("tbName", "newCol", "dtype")).thenReturn("sql");

        modifier.addProperty("tbName", "newCol", "dtype");

        verify(engine, times(1)).insertData("sql");
    }

    @Test
    void testAddPropertyNotAllowed() throws Exception {
        when(queryUtil.allowAddColumn()).thenReturn(false);

        modifier.addProperty("tbName", "newCol", "dtype");

        verify(engine, times(0)).insertData(anyString());
    }

    @Test
    void testAddPropertyException() throws Exception {
        when(queryUtil.allowAddColumn()).thenReturn(true);
        when(queryUtil.alterTableAddColumn("tbName", "newCol", "dtype")).thenReturn("sql");

        doThrow(new SQLException("err")).when(engine).insertData("sql");


        SQLException e = assertThrows(SQLException.class, () -> modifier.addProperty("tbName", "newCol", "dtype"));
        assertEquals("Error occurred to alter the table. See logs for details.", e.getMessage());
    }

    @Test
    void testEditProperty() throws Exception {
        when(queryUtil.allowAddColumn()).thenReturn(true);
        when(queryUtil.modColumnType("tbName", "newCol", "dtype")).thenReturn("sql");

        modifier.editProperty("tbName", "newCol", "dtype");

        verify(engine, times(1)).insertData("sql");
    }

    @Test
    void testEditPropertyNotAllowed() throws Exception {
        when(queryUtil.allowAddColumn()).thenReturn(false);

        modifier.editProperty("tbName", "newCol", "dtype");

        verify(engine, times(0)).insertData(anyString());
    }

    @Test
    void testEditPropertyException() throws Exception {
        when(queryUtil.allowAddColumn()).thenReturn(true);
        when(queryUtil.modColumnType("tbName", "newCol", "dtype")).thenReturn("sql");

        doThrow(new SQLException("err")).when(engine).insertData("sql");


        SQLException e = assertThrows(SQLException.class, () -> modifier.editProperty("tbName", "newCol", "dtype"));
        assertEquals("Error occurred to alter the table. See logs for details.", e.getMessage());
    }

    @Test
    void testRemoveProperty() throws Exception {
        when(queryUtil.allowDropColumn()).thenReturn(true);
        when(queryUtil.alterTableDropColumn("tbName", "col")).thenReturn("sql");

        modifier.removeProperty("tbName", "col");

        verify(engine, times(1)).insertData("sql");
    }

    @Test
    void testRemovePropertyNotAllowed() throws Exception {
        when(queryUtil.allowDropColumn()).thenReturn(false);

        modifier.removeProperty("tbName", "newCol");

        verify(engine, times(0)).insertData(anyString());
    }

    @Test
    void testRemove() throws Exception {
        when(queryUtil.allowDropColumn()).thenReturn(true);
        when(queryUtil.alterTableDropColumn("tbName", "col")).thenReturn("sql");

        doThrow(new SQLException("err")).when(engine).insertData("sql");

        SQLException e = assertThrows(SQLException.class, () -> modifier.removeProperty("tbName", "col"));
        assertEquals("Error occurred to alter the table. See logs for details.", e.getMessage());
    }

    @Test
    void testAddIndexIndexExists() throws Exception {
        when(queryUtil.createIndex("in", "tn", "cn")).thenReturn("iq");

        // a bunch of mocking here, but basically getting the wrapper to behave as intended
        try (MockedStatic<WrapperManager> wmStatic = Mockito.mockStatic(WrapperManager.class)) {
            WrapperManager wm = mock(WrapperManager.class);
            wmStatic.when(WrapperManager::getInstance).thenReturn(wm);

            String existingIndexQuery = "existing index query";
            when(queryUtil.allIndexForTableQuery("tn", "db", "schema")).thenReturn(existingIndexQuery);

            IRawSelectWrapper wrapper = mock(IRawSelectWrapper.class);
            when(wm.getRawWrapper(engine, existingIndexQuery)).thenReturn(wrapper);

            when(wrapper.hasNext()).thenReturn(true).thenReturn(false);
            IHeadersDataRow row = mock(IHeadersDataRow.class);

            // index name exists
            Object[] objects = { "in", "tn" };
            when(row.getValues()).thenReturn(objects);
            when(wrapper.next()).thenReturn(row);


            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> modifier.addIndex("tn", "cn", "in", false));
            assertEquals("The index name defined already exists", e.getMessage());
        }
    }

    @Test
    void testAddIndexTableExists() throws Exception {
        when(queryUtil.createIndex("in", "tn", "cn")).thenReturn("iq");

        // a bunch of mocking here, but basically getting the wrapper to behave as intended
        try (MockedStatic<WrapperManager> wmStatic = Mockito.mockStatic(WrapperManager.class)) {
            WrapperManager wm = mock(WrapperManager.class);
            wmStatic.when(WrapperManager::getInstance).thenReturn(wm);

            String existingIndexQuery = "existing index query";
            when(queryUtil.allIndexForTableQuery("tn", "db", "schema")).thenReturn(existingIndexQuery);

            IRawSelectWrapper wrapper = mock(IRawSelectWrapper.class);
            when(wm.getRawWrapper(engine, existingIndexQuery)).thenReturn(wrapper);

            when(wrapper.hasNext()).thenReturn(true).thenReturn(false);
            IHeadersDataRow row = mock(IHeadersDataRow.class);

            // New Index name, but table name exists
            Object[] objects = { "newIn", "tn" };
            when(row.getValues()).thenReturn(objects);
            when(wrapper.next()).thenReturn(row);


            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> modifier.addIndex("tn", "cn", "in", false));
            assertEquals("Index already exists on the column with the name 'tn'", e.getMessage());
        }
    }

    @Test
    void testAddIndexTableDoesNotExist() throws Exception {
        when(queryUtil.createIndex("in", "tn", "cn")).thenReturn("iq");

        // a bunch of mocking here, but basically getting the wrapper to behave as intended
        try (MockedStatic<WrapperManager> wmStatic = Mockito.mockStatic(WrapperManager.class)) {
            WrapperManager wm = mock(WrapperManager.class);
            wmStatic.when(WrapperManager::getInstance).thenReturn(wm);

            String existingIndexQuery = "existing index query";
            when(queryUtil.allIndexForTableQuery("tn", "db", "schema")).thenReturn(existingIndexQuery);

            IRawSelectWrapper wrapper = mock(IRawSelectWrapper.class);
            when(wm.getRawWrapper(engine, existingIndexQuery)).thenReturn(wrapper);

            when(wrapper.hasNext()).thenReturn(true).thenReturn(false);
            IHeadersDataRow row = mock(IHeadersDataRow.class);

            // both are new, no error
            Object[] objects = { "newIn", "newTn" };
            when(row.getValues()).thenReturn(objects);
            when(wrapper.next()).thenReturn(row);

            modifier.addIndex("tn", "cn", "in", false);

            verify(engine, times(1)).insertData("iq");
        }
    }

    @Test
    void testAddIndexAddIfExists() throws Exception {
        when(queryUtil.createIndex("in", "tn", "cn")).thenReturn("iq");

        // this tests keeps the mocking, even though we don't need it
        // but it ensures that the data is still added even if index exists
        try (MockedStatic<WrapperManager> wmStatic = Mockito.mockStatic(WrapperManager.class)) {
            WrapperManager wm = mock(WrapperManager.class);
            wmStatic.when(WrapperManager::getInstance).thenReturn(wm);

            String existingIndexQuery = "existing index query";
            when(queryUtil.allIndexForTableQuery("tn", "db", "schema")).thenReturn(existingIndexQuery);

            IRawSelectWrapper wrapper = mock(IRawSelectWrapper.class);
            when(wm.getRawWrapper(engine, existingIndexQuery)).thenReturn(wrapper);

            when(wrapper.hasNext()).thenReturn(true).thenReturn(false);
            IHeadersDataRow row = mock(IHeadersDataRow.class);

            // both are new, no error
            Object[] objects = { "in", "tn" };
            when(row.getValues()).thenReturn(objects);
            when(wrapper.next()).thenReturn(row);

            modifier.addIndex("tn", "cn", "in", true);

            verify(engine, times(1)).insertData("iq");
        }
    }

    @Test
    void testAddIndexSqlException() throws Exception {
        when(queryUtil.createIndex("in", "tn", "cn")).thenReturn("iq");

        doThrow(new SQLException("err")).when(engine).insertData("iq");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> modifier.addIndex("tn", "cn", "in", true));
        assertEquals("Could not add index. See logs for details.", e.getMessage());
    }

    @Test
    void testRenameProperty() throws Exception {
        when(queryUtil.modColumnName("eConcept", "eColumn", "nColumn")).thenReturn("query");
        modifier.renameProperty("eConcept", "eColumn", "nColumn");
        verify(engine, times(1)).insertData("query");
    }

    @Test
    void testRenamePropertyException() throws Exception {
        when(queryUtil.modColumnName("eConcept", "eColumn", "nColumn")).thenReturn("query");
        doThrow(new SQLException("err")).when(engine).insertData("query");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> modifier.renameProperty("eConcept", "eColumn", "nColumn"));
        assertEquals("Could not rename property. See logs for details.", e.getMessage());
    }

    @Test
    void testRenameConcept() throws Exception {
        when(queryUtil.alterTableName("ec", "nc")).thenReturn("query");
        modifier.renameConcept("ec", "nc");
        verify(engine, times(1)).insertData("query");
    }

    @Test
    void testRenameConceptException() throws Exception {
        when(queryUtil.alterTableName("ec", "nc")).thenReturn("query");
        doThrow(new SQLException("err")).when(engine).insertData("query");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> modifier.renameConcept("ec", "nc"));
        assertEquals("Could not alter table name. See logs for details.", e.getMessage());
    }

}
