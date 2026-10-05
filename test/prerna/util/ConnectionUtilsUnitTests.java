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

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import prerna.engine.api.IRDBMSEngine;

class ConnectionUtilsUnitTests {

	@Test
	void pooledReleaseClosesConnectionAndPropagatesFailure() throws Exception {
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		Connection connection = mock(Connection.class);
		when(engine.isConnectionPooling()).thenReturn(true);
		SQLException failure = new SQLException("release failed");
		doThrow(failure).when(connection).close();
		assertSame(failure,
				assertThrows(SQLException.class, () -> ConnectionUtils.closeConnectionIfPooling(engine, connection)));
		verify(connection).close();
	}

	@Test
	void sharedConnectionRemainsOpen() throws Exception {
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		Connection connection = mock(Connection.class);
		ConnectionUtils.closeConnectionIfPooling(engine, connection);
		verifyNoInteractions(connection);
	}
}
