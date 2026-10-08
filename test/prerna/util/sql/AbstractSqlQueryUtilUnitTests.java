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
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Types;
import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import prerna.engine.api.IRDBMSEngine;
import prerna.query.interpreters.sql.ParameterizedSqlInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;

class AbstractSqlQueryUtilUnitTests {

	@Test
	void h2RoundTripsExactValues() throws Exception {
		try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:binding_" + UUID.randomUUID())) {
			NullableParameterBindingUnitTests.roundTrip(connection, new H2QueryUtil());
		}
	}

	@Test
	void nullableStringVariantsPreserveTheirDistinctEmptyAndWhitespaceContracts() throws Exception {
		AbstractSqlQueryUtil[] dialects = { new H2QueryUtil(), new PostgresQueryUtil(), new SQLiteQueryUtil(),
				new MicrosoftSqlServerQueryUtil() };
		for (AbstractSqlQueryUtil dialect : dialects) {
			var ps = mock(java.sql.PreparedStatement.class);
			dialect.setNullableString(ps, 1, null);
			dialect.setNullableString(ps, 2, "");
			dialect.setNullableString(ps, 3, " 	 ");
			dialect.setStringEmptyAsNullable(ps, 4, null);
			dialect.setStringEmptyAsNullable(ps, 5, "");
			dialect.setStringEmptyAsNullable(ps, 6, " 	 ");
			dialect.setStringEmptyAsNullable(ps, 7, "  café 世界  ");
			verify(ps).setNull(1, Types.VARCHAR);
			verify(ps).setString(2, "");
			verify(ps).setString(3, " 	 ");
			verify(ps).setNull(4, Types.VARCHAR);
			verify(ps).setNull(5, Types.VARCHAR);
			verify(ps).setString(6, " 	 ");
			verify(ps).setString(7, "  café 世界  ");
			verifyNoMoreInteractions(ps);
		}
	}

	@Test
	void dialectSearchContractProducesBoundPatternsThroughTheInterpreter() {
		for (var type : RdbmsTypeEnum.values()) {
			AbstractSqlQueryUtil util = SqlQueryUtilFactory.initialize(type);
			var engine = mock(IRDBMSEngine.class);
			Mockito.when(engine.isBasic()).thenReturn(true);
			Mockito.when(engine.getQueryUtil()).thenReturn(util);
			var query = new SelectQueryStruct();
			query.addSelector(new QueryColumnSelector("ITEMS__V", "label"));
			query.addExplicitFilter(util.getPreparedSearchRegexFilter("ITEMS__V", "o'brien"));
			var compiled = new ParameterizedSqlInterpreter(engine).compile(query);
			Assertions.assertFalse(compiled.sql().contains("o'brien"));
			Assertions.assertTrue(compiled.parameters().stream()
					.anyMatch(value -> value instanceof String pattern && pattern.contains("o'brien")));
		}
	}

}
