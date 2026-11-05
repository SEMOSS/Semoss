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
package prerna.engine.impl.database.usage;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.usage.UsageLimitException;
import prerna.engine.impl.usage.UsageRule.Metric;

class RdbmsReadUsageCleanupUnitTests {
	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void admissionRefusalClosesStatementsAndOnlyPooledConnections(boolean pooled) throws Exception {
		var connection = mock(Connection.class);
		var statement = mock(Statement.class);
		when(connection.createStatement()).thenReturn(statement);
		var refusal = new UsageLimitException(Metric.QUERIES, Instant.parse("2026-10-16T00:00:00Z"));
		when(statement.executeQuery("SELECT 1")).thenThrow(refusal);
		var engine = new RDBMSNativeEngine() {
			{
				if (pooled)
					dataSource = mock(HikariDataSource.class);
			}

			@Override
			public Connection getConnection() {
				return connection;
			}
		};
		assertSame(refusal, assertThrows(UsageLimitException.class, () -> engine.execQuery("SELECT 1")));
		verify(statement).close();
		verify(connection, pooled ? times(1) : never()).close();
		verify(connection, never()).commit();
		verify(connection, never()).rollback();
	}
}
