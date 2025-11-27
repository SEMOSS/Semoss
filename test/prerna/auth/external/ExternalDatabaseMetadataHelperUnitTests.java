package prerna.auth.external;

import org.junit.jupiter.api.Test;
import prerna.engine.impl.AbstractDatabaseEngine;

import java.util.Vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExternalDatabaseMetadataHelperUnitTests {

    @Test
    void parseJsonToOwlException() {
        AbstractDatabaseEngine database = new AbstractDatabaseEngine() {
            @Override
            public boolean holdsFileLocks() {
                return false;
            }

            @Override
            public Object execQuery(String query) throws Exception {
                return null;
            }

            @Override
            public void insertData(String query) throws Exception {

            }

            @Override
            public void removeData(String query) throws Exception {

            }

            @Override
            public void commit() {

            }

            @Override
            public DATABASE_TYPE getDatabaseType() {
                return null;
            }

            @Override
            public Vector<Object> getEntityOfType(String type) {
                return null;
            }
        };


        Exception e = assertThrows(Exception.class, () -> ExternalDatabaseMetadataHelper.parseJsonToOwl(database));
        assertEquals("Null key.", e.getMessage());
    }
}
