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
package prerna.reactor.apptest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import prerna.query.parsers.SqlScriptSplitter;

/** One regression run owns this database and its non-admin JDBC identity. */
final class RegressionH2Database implements AutoCloseable {
    private static final String ID = "(?:[A-Za-z][A-Za-z0-9_]{0,62}|\"[A-Za-z][A-Za-z0-9_ ]{0,62}\")";
    private static final String IDS = ID + "(?:\\s*,\\s*" + ID + ")*";
    private static final String VALUE = "(?:NULL|TRUE|FALSE|-?\\d{1,19}(?:\\.\\d{1,16})?|'(?:[^'\\x00]++|'')*')";
    private static final String VALUES = VALUE + "(?:\\s*,\\s*" + VALUE + ")*";
    private static final Pattern INSERT = sql("INSERT\\s+INTO\\s+" + ID + "(?:\\s*\\(" + IDS
            + "\\))?\\s+VALUES\\s*\\(" + VALUES + "\\)");
    private static final String TYPE = "(?:BIGINT|INTEGER|INT|BOOLEAN|DATE|TIMESTAMP|VARCHAR\\(\\d{1,5}\\)|DECIMAL\\(\\d{1,2},\\s*\\d{1,2}\\))";
    private static final String CHECK = "CHECK\\s*\\(\\s*" + ID + "\\s+IN\\s*\\(" + VALUES + "\\)\\s*\\)";
    private static final String COLUMN = ID + "\\s+" + TYPE + "(?:\\s+(?:NOT NULL|PRIMARY KEY|UNIQUE|" + CHECK + "))*";
    private static final String CONSTRAINT = "(?:PRIMARY KEY\\s*\\(" + IDS + "\\)|UNIQUE\\s*\\(" + IDS
            + "\\)|FOREIGN KEY\\s*\\(" + IDS + "\\)\\s+REFERENCES\\s+" + ID + "\\s*\\(" + IDS + "\\)(?:\\s+ON\\s+DELETE\\s+(?:CASCADE|RESTRICT|SET NULL))?)";
    private static final String TABLE_ITEM = "(?:" + COLUMN + "|" + CONSTRAINT + ")";
    private static final Pattern TABLE = sql("CREATE\\s+TABLE\\s+(?:IF NOT EXISTS\\s+)?" + ID
            + "\\s*\\(\\s*" + TABLE_ITEM + "(?:\\s*,\\s*" + TABLE_ITEM + ")*\\s*\\)");
    private static final Pattern INDEX = sql("CREATE\\s+(?:UNIQUE\\s+)?INDEX\\s+" + ID
            + "\\s+ON\\s+" + ID + "\\s*\\(\\s*" + IDS + "\\s*\\)");
    private static final Pattern ADD_COLUMN = sql("ALTER\\s+TABLE\\s+" + ID + "\\s+ADD\\s+(?:COLUMN\\s+)?" + COLUMN);
    private static final String USER = "REGRESSION_APP";
    private static final int MAX_SQL_CHARS = 4096;
    private static final int MAX_ROWS = 100;

    private final Path directory;
    private final Connection connection;

    private RegressionH2Database(Path directory, Connection connection) {
        this.directory = directory;
        this.connection = connection;
    }

    static RegressionH2Database open(Path root, String baseline, String migration, String seed)
            throws IOException, SQLException {
        Path directory = Files.createTempDirectory(root, "regression-h2-");
        String url = "jdbc:h2:file:" + directory.resolve("data").toString().replace('\\', '/');
        String adminPassword = UUID.randomUUID().toString();
        String appPassword = UUID.randomUUID().toString();
        try {
            try (Connection admin = DriverManager.getConnection(url, "SA", adminPassword)) {
                apply(admin, baseline, List.of(TABLE, INDEX));
                if (!migration.isBlank()) {
                    apply(admin, migration, List.of(TABLE, INDEX, ADD_COLUMN));
                }
                if (!seed.isBlank()) apply(admin, seed, List.of(INSERT));
                try (var createUser = admin.prepareStatement("CREATE USER " + USER + " PASSWORD ?")) {
                    createUser.setString(1, appPassword);
                    createUser.execute();
                }
                try (var statement = admin.createStatement()) {
                    statement.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA PUBLIC TO " + USER);
                }
            }
            return new RegressionH2Database(directory, DriverManager.getConnection(url + ";IFEXISTS=TRUE", USER, appPassword));
        } catch (IOException | SQLException | RuntimeException e) {
            try {
                delete(directory);
            } catch (IOException cleanup) {
                e.addSuppressed(cleanup);
            }
            throw e;
        }
    }

    private static Pattern sql(String expression) {
        return Pattern.compile(expression, Pattern.CASE_INSENSITIVE);
    }

    private static void apply(Connection connection, String script, List<Pattern> allowed) throws SQLException, IOException {
        var statements = SqlScriptSplitter.split(script);
        if (statements.isEmpty() || statements.size() > 256) {
            throw new IOException("A fixture must have between 1 and 256 SQL statements");
        }
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(5);
            for (var item : statements) {
                String sql = item.body();
                if (sql.length() > MAX_SQL_CHARS || allowed.stream().noneMatch(pattern -> pattern.matcher(sql).matches())) {
                    throw new IOException("Test database SQL contains an unsupported statement. Use simple CREATE TABLE/INDEX for schema and literal INSERT statements for seed data.");
                }
                statement.execute(sql);
            }
        }
    }

    static boolean isRead(String sql) {
        return RegressionSql.isRead(sql);
    }

    synchronized Object execute(String sql, int limit) throws SQLException {
        boolean read = isRead(sql);
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(5);
            if (!read) {
                statement.executeUpdate(sql);
                return true;
            }
            statement.setMaxRows(Math.clamp(limit, 1, MAX_ROWS));
            try (ResultSet rows = statement.executeQuery(sql)) {
                var columns = new ArrayList<String>();
                var types = new ArrayList<String>();
                var metadata = rows.getMetaData();
                for (int column = 1; column <= metadata.getColumnCount(); column++) {
                    columns.add(metadata.getColumnLabel(column));
                    types.add(metadata.getColumnTypeName(column));
                }
                List<List<Object>> values = new ArrayList<>();
                while (rows.next()) {
                    var row = new ArrayList<>();
                    for (int column = 1; column <= columns.size(); column++) {
                        Object value = rows.getObject(column);
                        row.add(value == null || value instanceof String || value instanceof Number
                                || value instanceof Boolean ? value : value.toString());
                    }
                    values.add(row);
                }
                return Map.of("data", Map.of("headers", columns, "rawHeaders", columns, "values", values, "types", types));
            }
        }
    }

    @Override
    public synchronized void close() throws IOException, SQLException {
        connection.close();
        delete(directory);
    }

    private static void delete(Path directory) throws IOException {
        try (var files = Files.walk(directory)) {
            for (Path file : files.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(file);
            }
        }
    }
}
