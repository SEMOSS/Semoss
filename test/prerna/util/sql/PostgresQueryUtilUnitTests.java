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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.PreparedStatement;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import prerna.query.querystruct.filters.SimpleQueryFilter;

class PostgresQueryUtilUnitTests {

	@Test
	void postgresUsesTextAndBytesForNonNullValues() throws Exception {
		PreparedStatement ps = mock(PreparedStatement.class);
		PostgresQueryUtil util = new PostgresQueryUtil();
		byte[] bytes = { 0, -1, -128 };
		util.setNullableLargeText(ps, 1, "  text  ");
		util.setNullableBinary(ps, 2, bytes);
		verify(ps).setString(1, "  text  ");
		verify(ps).setBytes(2, bytes);
	}

	@Test
	void preparedRegexPreservesPatternSyntaxAndUsesBoundComparison() {
		var util = new PostgresQueryUtil();
		var filter = (SimpleQueryFilter) util.getPreparedSearchRegexFilter("ITEMS__V", "O'Brien\\path");
		Assertions.assertEquals("o'brien\\path", filter.getRComparison().getValue());
		Assertions.assertEquals("~", filter.getComparator());
	}

}
