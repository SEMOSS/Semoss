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
package prerna.util;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;

import org.mockito.MockedStatic;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/**
 * Disposable in-memory database for public utility regression tests. Never
 * loads a system database.
 */
public final class JdbcTestDatabase implements AutoCloseable {

	public final Connection connection;
	public final IRDBMSEngine engine;
	public final MockedStatic<SystemEngineRegistry> registry;

	public JdbcTestDatabase() throws Exception {
		connection = spy(DriverManager.getConnection("jdbc:h2:mem:jdbc_utils_" + UUID.randomUUID()));
		engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.getQueryUtil()).thenReturn(SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB));
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(SystemEngineRegistry::getSecurityDb).thenReturn(engine);
		registry.when(SystemEngineRegistry::getSchedulerDb).thenReturn(engine);
		registry.when(SystemEngineRegistry::getThemesDb).thenReturn(engine);
		registry.when(SystemEngineRegistry::getUserTrackingDb).thenReturn(engine);
		registry.when(SystemEngineRegistry::getPromptDb).thenReturn(engine);
		registry.when(SystemEngineRegistry::getNotificationDb).thenReturn(engine);
		registry.when(SystemEngineRegistry::getModelInferenceLogsDb).thenReturn(engine);
		registry.when(SystemEngineRegistry::getLocalMasterDb).thenReturn(engine);
	}

	public void execute(String... sql) throws Exception {
		try (Statement statement = connection.createStatement()) {
			for (String query : sql) {
				statement.execute(query);
			}
		}
	}

	public Object value(String sql) throws Exception {
		try (Statement statement = connection.createStatement(); ResultSet result = statement.executeQuery(sql)) {
			if (!result.next()) {
				return null;
			}
			Object value = result.getObject(1);
			// Materialize LOBs before the result set closes, just as product callers do.
			if (value instanceof Clob) {
				return result.getString(1);
			}
			if (value instanceof Blob) {
				return result.getBytes(1);
			}
			return value;
		}
	}

	public int count(String table) throws Exception {
		return ((Number) value("SELECT COUNT(*) FROM " + table)).intValue();
	}

	public void manual() throws Exception {
		connection.setAutoCommit(false);
		clearInvocations(connection, engine);
	}

	@Override
	public void close() throws Exception {
		registry.close();
		connection.close();
	}
}
