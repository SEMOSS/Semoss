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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.sql.PreparedStatement;
import java.sql.Types;

import org.junit.jupiter.api.Test;

class MicrosoftSqlServerQueryUtilUnitTests {

	@Test
	void maxColumnBindingsPreserveExactTextBytesAndTypedNulls() throws Exception {
		var util = new MicrosoftSqlServerQueryUtil();
		var statement = mock(PreparedStatement.class);
		String text = "  body  ";
		byte[] bytes = { 0, -1, -128, 42 };
		util.setNullableLargeText(statement, 1, text);
		util.setNullableLargeText(statement, 2, null);
		util.setNullableBinary(statement, 3, bytes);
		util.setNullableBinary(statement, 4, null);
		verify(statement).setString(1, text);
		verify(statement).setNull(2, Types.LONGVARCHAR);
		verify(statement).setBytes(3, bytes);
		verify(statement).setNull(4, Types.LONGVARBINARY);
		verifyNoMoreInteractions(statement);
		assertTrue(util.getClobDataTypeName().toUpperCase().contains("VARCHAR"));
		assertTrue(util.getBlobDataTypeName().toUpperCase().contains("VARBINARY"));
	}
}
