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
package prerna.util.sql;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

import prerna.engine.api.IRDBMSEngine;
import prerna.query.interpreters.sql.ParameterizedSqlInterpreterUnitTests;

class SqlServerParameterizedQueryUnitTests {

	@Test
	void disposableSqlServerExecutesCompiledQueriesAndReleasesReadTransactions() throws Exception {
		assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Disposable SQL Server requires Docker");
		String password = "Local-Jdbc-Test-42!";
		try (var server = new GenericContainer<>("mcr.microsoft.com/mssql/server:2022-latest")
				.withEnv("ACCEPT_EULA", "Y").withEnv("MSSQL_PID", "Developer").withEnv("MSSQL_SA_PASSWORD", password)
				.withExposedPorts(1433).withCreateContainerCmdModifier(command -> command.withPlatform("linux/amd64"))
				.waitingFor(Wait.forLogMessage(".*SQL Server is now ready for client connections.*", 1))
				.withStartupTimeout(Duration.ofMinutes(3))) {
			server.start();
			var dataSource = new SQLServerDataSource();
			dataSource.setURL("jdbc:sqlserver://" + server.getHost() + ":" + server.getMappedPort(1433)
					+ ";encrypt=true;trustServerCertificate=true");
			dataSource.setLoginTimeout(20);
			dataSource.setSocketTimeout(30_000);
			try (var connection = dataSource.getConnection("sa", password)) {
				IRDBMSEngine engine = mock(IRDBMSEngine.class);
				when(engine.getConnection()).thenReturn(connection);
				when(engine.isBasic()).thenReturn(true);
				when(engine.getQueryUtil()).thenReturn(new MicrosoftSqlServerQueryUtil());
				when(engine.getDatabaseZoneId()).thenReturn(ZoneOffset.UTC);
				ParameterizedSqlInterpreterUnitTests.roundTrip(engine);
				assertTrue(connection.getAutoCommit());
				assertFalse(connection.isClosed());
			}
		}
	}
}
