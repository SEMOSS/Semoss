/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.ds.rdbms;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.Timestamp;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.ds.shared.CachedIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.HeadersException;
import prerna.rdf.engine.wrappers.RawRDBMSSelectWrapper;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class RdbmsFrameBuilderUnitTests {
    private Connection conn;
    private AbstractSqlQueryUtil absSqlQueryUtil;
    private PreparedStatement ps;
    private Statement statement;
    private HeadersException headerException;
    
    private RdbmsFrameBuilder reactor;

    private final String TABLE_NAME = "TABLE";
    private final String DATABASE_NAME = "DATABASE";
    private final String SCHEMA = "SCHEMA";
    private final String[] cols = new String[]{"col1"};
    private final Object[] vals = new Object[]{1};
    private final String[] types = new String[]{"int"};


    @BeforeEach
    void setup() {
        conn = mock(Connection.class);
        ps = mock(PreparedStatement.class);
        absSqlQueryUtil = mock(AbstractSqlQueryUtil.class);
        statement = mock(Statement.class);

        reactor = new RdbmsFrameBuilder(conn, DATABASE_NAME, SCHEMA, absSqlQueryUtil);
    }

    @Test
    void setLogger() {
        reactor.setLogger(null);
    }

    @Test
    void returnsPs() throws Exception {
        String[] where = new String[]{"col1"};
        String insertQuery = "INSERT INTO TABLE (?) VALUES (?)";
        String updateQuery = "UPDATE TABLE SET ? = ? WHERE ? = ?";
        String hashColQuery = "HASH COLUMN ? FROM TABLE";

        when(absSqlQueryUtil.createInsertPreparedStatementString(TABLE_NAME, cols)).thenReturn(insertQuery);
        when(conn.prepareStatement(insertQuery)).thenReturn(ps);
        
        when(absSqlQueryUtil.createUpdatePreparedStatementString(TABLE_NAME, cols, where)).thenReturn(updateQuery);
        when(conn.prepareStatement(updateQuery)).thenReturn(ps);
        
        when(absSqlQueryUtil.hashColumn(TABLE_NAME, cols)).thenReturn(hashColQuery);
        when(conn.prepareStatement(hashColQuery)).thenReturn(ps);

        PreparedStatement instert = reactor.createInsertPreparedStatement(TABLE_NAME, cols);
        PreparedStatement update = reactor.createUpdatePreparedStatement(TABLE_NAME, cols, where);
        PreparedStatement hash = reactor.hashColumn(TABLE_NAME, cols);

        assertNotNull(reactor);
        assertNotNull(instert);
        assertNotNull(update);
        assertNotNull(hash);
    }

    @Test
    void returnsPsException() throws Exception {
        String[] where = new String[]{"col1"};
        String insertQuery = "INSERT INTO TABLE (?) VALUES (?)";
        String updateQuery = "UPDATE TABLE SET ? = ? WHERE ? = ?";
        String hashColQuery = "HASH COLUMN ? FROM TABLE";

        when(absSqlQueryUtil.createInsertPreparedStatementString(TABLE_NAME, cols)).thenReturn(insertQuery);
        when(conn.prepareStatement(insertQuery)).thenThrow(new SQLException());
        
        when(absSqlQueryUtil.createUpdatePreparedStatementString(TABLE_NAME, cols, where)).thenReturn(updateQuery);
        when(conn.prepareStatement(updateQuery)).thenThrow(new SQLException());
        
        when(absSqlQueryUtil.hashColumn(TABLE_NAME, cols)).thenReturn(hashColQuery);
        when(conn.prepareStatement(hashColQuery)).thenThrow(new SQLException());

        PreparedStatement instert = reactor.createInsertPreparedStatement(TABLE_NAME, cols);
        PreparedStatement update = reactor.createUpdatePreparedStatement(TABLE_NAME, cols, where);
        PreparedStatement hash = reactor.hashColumn(TABLE_NAME, cols);

        assertNull(instert);
        assertNull(update);
        assertNull(hash);
    }

    @Test
    void columnIndexedTest() {
        boolean ans = reactor.columnIndexed(TABLE_NAME, "col1");

        assertFalse(ans);
    }

    @Test
    void getHeadersTest() {
        when(absSqlQueryUtil.getTableColumns(conn, TABLE_NAME, DATABASE_NAME, SCHEMA)).thenReturn(new ArrayList(){{add("col1");}});

        String[] ans = reactor.getHeaders(TABLE_NAME);

        assertNotNull(ans);
        assertEquals("[col1]", Arrays.toString(ans));
    }

    @Test
    void isEmptyTest() throws Exception {
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(absSqlQueryUtil.tableExists(conn, TABLE_NAME, DATABASE_NAME, SCHEMA)).thenReturn(true);
        when(conn.prepareStatement("SELECT * FROM " + TABLE_NAME + " LIMIT 1")).thenReturn(stmt);
        when(stmt.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        doNothing().when(rs).close();
        doNothing().when(stmt).close();

        boolean ans = reactor.isEmpty(TABLE_NAME);

        assertFalse(ans);
    }

    @Test
    void isEmptyTestException() throws Exception {
        when(absSqlQueryUtil.tableExists(conn, TABLE_NAME, DATABASE_NAME, SCHEMA)).thenReturn(true);
        when(conn.prepareStatement(anyString())).thenThrow(new SQLException());

        boolean ans = reactor.isEmpty(TABLE_NAME);

        assertTrue(ans);
    }

    @Test
    void isEmptyTestCloseException() throws Exception {
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(absSqlQueryUtil.tableExists(conn, TABLE_NAME, DATABASE_NAME, SCHEMA)).thenReturn(true);
        when(conn.prepareStatement("SELECT * FROM " + TABLE_NAME + " LIMIT 1")).thenReturn(stmt);
        when(stmt.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        doThrow(new SQLException()).when(rs).close();
        doThrow(new SQLException()).when(stmt).close();

        boolean ans = reactor.isEmpty(TABLE_NAME);

        assertFalse(ans);
    }

    @Test
    void getNumRecordsTest() throws Exception {
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(absSqlQueryUtil.tableExists(conn, TABLE_NAME, DATABASE_NAME, SCHEMA)).thenReturn(true);
        when(conn.prepareStatement("SELECT COUNT(*) * 0 FROM " + TABLE_NAME)).thenReturn(stmt);
        when(stmt.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getInt(1)).thenReturn(1);
        doNothing().when(rs).close();
        doNothing().when(stmt).close();

        int ans = reactor.getNumRecords(TABLE_NAME);

        assertEquals(1, ans);
    }

    @Test
    void getNumRecordsExceptionTest() throws Exception {
        when(absSqlQueryUtil.tableExists(conn, TABLE_NAME, DATABASE_NAME, SCHEMA)).thenReturn(true);
        when(conn.prepareStatement(anyString())).thenThrow(new SQLException());

        int ans = reactor.getNumRecords(TABLE_NAME);

        assertEquals(0, ans);
    }

    @Test
    void getNumRecordsTestCloseException() throws Exception {
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(absSqlQueryUtil.tableExists(conn, TABLE_NAME, DATABASE_NAME, SCHEMA)).thenReturn(true);
        when(conn.prepareStatement("SELECT COUNT(*) * 0 FROM " + TABLE_NAME)).thenReturn(stmt);
        when(stmt.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getInt(1)).thenReturn(1);
        doThrow(new SQLException()).when(rs).close();
        doThrow(new SQLException()).when(stmt).close();

        int ans = reactor.getNumRecords(TABLE_NAME);

        assertEquals(1, ans);
    }

    @Test
    void addRowTest() throws Exception {
        String createQuery = "CREATE TABLE TABLE COL1 INT";
        String insertQuery = "INSERT INTO TABLE (?) VALUES (?)";

        when(absSqlQueryUtil.cleanTypes(types)).thenReturn(types);
        when(absSqlQueryUtil.tableExists(conn, TABLE_NAME, DATABASE_NAME, SCHEMA)).thenReturn(false);
        when(absSqlQueryUtil.createTable(TABLE_NAME, cols, types)).thenReturn(createQuery);
        when(absSqlQueryUtil.insertIntoTable(TABLE_NAME, cols, types, vals)).thenReturn(insertQuery);

        when(conn.createStatement()).thenReturn(statement);
        when(statement.executeUpdate(createQuery))
            .thenThrow(new SQLException())
            .thenReturn(1);

        when(conn.prepareStatement(insertQuery)).thenReturn(ps);
        when(ps.execute())
            .thenReturn(true)
            .thenThrow(new SQLException());

        reactor.addRow(TABLE_NAME, cols, vals, types);
        
        verify(absSqlQueryUtil).cleanTypes(types);
        verify(absSqlQueryUtil).tableExists(conn, TABLE_NAME, DATABASE_NAME, SCHEMA);
        verify(absSqlQueryUtil).createTable(TABLE_NAME, cols, types);

        reactor.addRow(TABLE_NAME, cols, vals, types);

        verify(absSqlQueryUtil, times(2)).cleanTypes(types);
        verify(absSqlQueryUtil, times(2)).tableExists(conn, TABLE_NAME, DATABASE_NAME, SCHEMA);
        verify(absSqlQueryUtil, times(2)).createTable(TABLE_NAME, cols, types);
        verify(absSqlQueryUtil, times(1)).insertIntoTable(TABLE_NAME, cols, types, vals);

        reactor.addRow(TABLE_NAME, cols, vals, types);

        verify(absSqlQueryUtil, times(3)).cleanTypes(types);
        verify(absSqlQueryUtil, times(3)).tableExists(conn, TABLE_NAME, DATABASE_NAME, SCHEMA);
        verify(absSqlQueryUtil, times(3)).createTable(TABLE_NAME, cols, types);
        verify(absSqlQueryUtil, times(2)).insertIntoTable(TABLE_NAME, cols, types, vals);
    }

    @Test
    void addRowsViaIteratorTest() throws Exception {
        Date date = new Date();
        List<Map<String, Object>> list = new ArrayList<>();
            //add(new HashMap<>() {{
                //put("alias", "header1");
            //}});
        for (int i = 1; i <= 21; i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("alias", "header" + i);
            list.add(map);
        }

        Map<String, SemossDataType> typesMap = new HashMap<>();
        SemossDataType[] semossDataTypes = new SemossDataType[]{
                SemossDataType.INT, SemossDataType.INT, SemossDataType.INT,
                SemossDataType.DOUBLE, SemossDataType.DOUBLE, SemossDataType.DOUBLE,
                SemossDataType.DATE, SemossDataType.DATE, SemossDataType.DATE, SemossDataType.DATE, SemossDataType.DATE,
                SemossDataType.TIMESTAMP, SemossDataType.TIMESTAMP, SemossDataType.TIMESTAMP, SemossDataType.TIMESTAMP, SemossDataType.TIMESTAMP,
                SemossDataType.BOOLEAN, SemossDataType.BOOLEAN,
                SemossDataType.STRING, SemossDataType.STRING, SemossDataType.STRING
        };
        for (int i = 1; i <= 21; i++ ) {
            String alias = "header" + i;
            typesMap.put(alias, semossDataTypes[(i - 1)]);
        }

        BasicIteratorTask basicIt = mock(BasicIteratorTask.class);
        CachedIterator cachedIterator = mock(CachedIterator.class);
        IHeadersDataRow iHeadersDataRow = mock(IHeadersDataRow.class);
        RawRDBMSSelectWrapper rawWrapper = mock(RawRDBMSSelectWrapper.class);

        headerException = mock(HeadersException.class);

        String[] headers = new String[]{
            "header1", "header2", "header3", "header4", "header5",
            "header6", "header7", "header8", "header9", "header10",
            "header11", "header12", "header13", "header14", "header15",
            "header16", "header17", "header18", "header19", "header20", "header21"
        };

        Object[] obj = new Object[]{
                1, "2", null,
                1.0, "2.0", null, 
                null, new SemossDate(date), new SemossDate(date), "2025-01-01", "",
                null, new SemossDate(date), new SemossDate(date), "2026-01-01 09:00:00", "",
                null, true,
                null, new Object[]{"objArray"}, "string"
            };

        try (MockedStatic<HeadersException> headerExceptionStatic = Mockito.mockStatic(HeadersException.class);
            MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
            MockedStatic<SemossDate> staticDate = Mockito.mockStatic(SemossDate.class)) {
            when(rawWrapper.getHeaders()).thenReturn(headers);
            when(basicIt.getHeaderInfo()).thenReturn(list);

            headerExceptionStatic.when(() -> HeadersException.getInstance()).thenReturn(headerException);
            when(headerException.getCleanHeaders(headers)).thenReturn(headers);

            // helper method
            alterTableNewColumnsTest(TABLE_NAME, headers, types);
            
            when(basicIt.hasNext()).thenReturn(true).thenReturn(false);
            when(rawWrapper.hasNext()).thenReturn(true).thenReturn(false);
            when(cachedIterator.hasNext()).thenReturn(true).thenReturn(false);
            when(basicIt.next()).thenReturn(iHeadersDataRow);
            when(rawWrapper.next()).thenReturn(iHeadersDataRow);
            when(cachedIterator.next()).thenReturn(iHeadersDataRow);

            when(iHeadersDataRow.getValues()).thenReturn(obj);

            when(iHeadersDataRow.getHeaders()).thenReturn(headers);

            when(absSqlQueryUtil.createInsertPreparedStatementString(TABLE_NAME, headers)).thenReturn("INSERT INTO TABLE (?) VALUES (?)");
            when(conn.prepareStatement("INSERT INTO TABLE (?) VALUES (?)")).thenReturn(ps);

            util.when(() -> Utility.getInteger("2")).thenReturn(2);
            util.when(() -> Utility.getInteger(null + "")).thenReturn(null);
            doNothing().when(ps).setInt(anyInt(), anyInt());
            doNothing().when(ps).setNull(anyInt(), eq(java.sql.Types.INTEGER));

            util.when(() -> Utility.getDouble("2.0")).thenReturn(2.0);
            util.when(() -> Utility.getDouble(null + "")).thenReturn(null);
            doNothing().when(ps).setDouble(anyInt(), any(Double.class));
            doNothing().when(ps).setNull(anyInt(), eq(java.sql.Types.DOUBLE));

            SemossDate mockDate = mock(SemossDate.class);
            when(mockDate.getDate()).thenReturn(date).thenReturn(null);
            staticDate.when(() -> SemossDate.genDateObj("2025-01-01")).thenReturn(mockDate);
            when(mockDate.getDate()).thenReturn(date);
            staticDate.when(() -> SemossDate.genDateObj("")).thenReturn(null);
            doNothing().when(ps).setDate(anyInt(), any(java.sql.Date.class));
            doNothing().when(ps).setNull(anyInt(), eq(java.sql.Types.DATE));

            staticDate.when(() -> SemossDate.genTimeStampDateObj("2026-01-01 09:00:00")).thenReturn(mockDate);
            when(mockDate.getDate()).thenReturn(date).thenReturn(date);
            staticDate.when(() -> SemossDate.genTimeStampDateObj("")).thenReturn(null);
            doNothing().when(ps).setTimestamp(anyInt(), any(java.sql.Timestamp.class));
            doNothing().when(ps).setNull(anyInt(), eq(java.sql.Types.TIMESTAMP));

            doNothing().when(ps).setBoolean(anyInt(), any(Boolean.class));
            doNothing().when(ps).setNull(anyInt(), eq(java.sql.Types.BOOLEAN));

            doNothing().when(ps).setString(anyInt(), anyString());
            doNothing().when(ps).setNull(anyInt(), eq(java.sql.Types.VARCHAR));

            doNothing().when(ps).addBatch();
            when(ps.executeBatch()).thenReturn(new int[0]);
            doNothing().when(ps).close();

            reactor.addRowsViaIterator(basicIt, TABLE_NAME, typesMap);
            reactor.addRowsViaIterator(rawWrapper, TABLE_NAME, typesMap);
            reactor.addRowsViaIterator(cachedIterator, TABLE_NAME, typesMap);
        }
    }

    @Test
    void alterTableNewColumnsTest() throws Exception {
        String createQuery = "CREATE TABLE TABLE COL1 INT";
        String alterQuery = "ALTER TABLE " + TABLE_NAME + " ADD COLUMN col1 int";

        addColumnIndex(TABLE_NAME, cols[0]);

        when(absSqlQueryUtil.cleanTypes(types)).thenReturn(types);
        when(absSqlQueryUtil.tableExists(conn, TABLE_NAME, DATABASE_NAME, SCHEMA))
            .thenReturn(false)
            .thenReturn(false)
            .thenReturn(true);

        when(absSqlQueryUtil.createTable(TABLE_NAME, cols, types)).thenReturn(createQuery);

        when(absSqlQueryUtil.getTableColumns(conn, TABLE_NAME, DATABASE_NAME, SCHEMA)).thenReturn(
                new ArrayList(){{add("COL");}}
        );

        when(absSqlQueryUtil.allowMultiAddColumn())
            .thenReturn(true)
            .thenReturn(false);
        when(absSqlQueryUtil.alterTableAddColumns(TABLE_NAME, new String[]{"col1"}, types)).thenReturn(
            "ALTER TABLE " + TABLE_NAME + " ADD COLUMN col1 int"
        );
        when(absSqlQueryUtil.alterTableAddColumn(TABLE_NAME, "col1", "int")).thenReturn(
            "ALTER TABLE " + TABLE_NAME + " ADD COLUMN col1 int"
        );
        
        runQuery(createQuery);
        removeColumnIndex(TABLE_NAME, "col1");
        runQuery(alterQuery);

        reactor.alterTableNewColumns(TABLE_NAME, cols, types);
        reactor.alterTableNewColumns(TABLE_NAME, cols, types);
        reactor.alterTableNewColumns(TABLE_NAME, cols, types);
        reactor.alterTableNewColumns(TABLE_NAME, cols, types);
        reactor.alterTableNewColumns(TABLE_NAME, cols, types);
        reactor.alterTableNewColumns(TABLE_NAME, cols, types);
    }

    @Test
    void removeAllIndexes() throws Exception {
        String[] cols2 = new String[]{"col1", "col2"};

        addColumnIndex(TABLE_NAME, cols[0]);
        addColumnIndex(TABLE_NAME, cols2);

        try {
            removeColumnIndex(TABLE_NAME, cols2);
        } catch (Exception e) {
        }

        reactor.removeAllIndexes();

        addColumnIndex(TABLE_NAME, cols2);
        reactor.removeAllIndexes();
    }

    //////////////////////////////
    /// Private Helper Methods ///
    //////////////////////////////

    private void runQuery(String query) throws Exception {
        when(conn.createStatement()).thenReturn(statement);
        when(statement.executeUpdate(query))
            .thenThrow(new SQLException())
            .thenReturn(1);

        when(conn.prepareStatement(query)).thenReturn(ps);
        when(ps.execute()).thenReturn(true);
    }

    private void removeColumnIndex(String tableName, String colName) throws Exception {
        String dropQuery = "DROP INDEX IDX ON " + tableName;

        when(absSqlQueryUtil.dropIndex(anyString(), eq(tableName)))
            .thenThrow(new NullPointerException())
            .thenReturn(dropQuery);

        runQuery(dropQuery);
    }

    private void removeColumnIndex(String tableName, String[] colName) throws Exception {
        String dropQuery = "DROP INDEX IDX ON " + tableName;

        when(absSqlQueryUtil.dropIndex(any(String.class), eq(tableName))).thenReturn(dropQuery);

        runQuery(dropQuery);
    }

    private void addColumnIndex(String tableName, String colName) throws Exception {
        String query = "CREATE INDEX IDX ON " + tableName;

        when(absSqlQueryUtil.createIndex(anyString(), eq(tableName), eq(colName))).thenReturn(query);

        runQuery(query);

        reactor.addColumnIndex(tableName, colName);
        reactor.addColumnIndex(tableName, colName);
    }

    private void addColumnIndex(String tableName, String[] colName) throws Exception {
        String query = "CREATE INDEX IDX ON " + tableName;

        when(absSqlQueryUtil.createIndex(anyString(), eq(tableName), eq(Arrays.asList(colName)))).thenReturn(query);

        runQuery(query);

        reactor.addColumnIndex(tableName, colName);
        reactor.addColumnIndex(tableName, colName);
    }

    private void alterTableNewColumnsTest(String table, String[] headers, String[] types) throws Exception {
        String createQuery = "CREATE TABLE TABLE COL1 INT";

        addColumnIndex(table, headers);

        when(absSqlQueryUtil.cleanTypes(types)).thenReturn(types);
        when(absSqlQueryUtil.tableExists(conn, table, DATABASE_NAME, SCHEMA))
            .thenReturn(false);

        when(absSqlQueryUtil.createTable(table, cols, types)).thenReturn(createQuery);
        
        when(conn.createStatement()).thenReturn(statement);
        when(statement.executeUpdate(createQuery))
            .thenReturn(1);

        when(conn.prepareStatement(createQuery)).thenReturn(ps);
        when(ps.execute()).thenReturn(true);
    }
}
