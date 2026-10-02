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
package prerna.reactor.qs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AbstractSqlQueryReactorUnitTests {

	@ParameterizedTest
	@ValueSource(strings = { "SELECT id FROM orders", "/* leading comment */ SELECT id FROM orders",
			"WITH active AS (SELECT id FROM orders) SELECT id FROM active", "SHOW TABLES", "DESCRIBE orders",
			"EXPLAIN SELECT id FROM orders", "VALUES (1)" })
	void parsedReadStatementsUseTheReadRoute(String query) {
		assertEquals(AbstractSqlQueryReactor.QueryRoute.READ, AbstractSqlQueryReactor.detectQueryRoute(query, false));
	}

	@ParameterizedTest
	@ValueSource(strings = { "INSERT INTO orders(id) VALUES (1)", "UPDATE orders SET status = 'open' WHERE id = 1",
			"DELETE FROM orders WHERE id = 1", "CREATE TABLE orders(id INTEGER)", "TRUNCATE TABLE orders" })
	void parsedMutationStatementsUseTheWriteRoute(String query) {
		assertEquals(AbstractSqlQueryReactor.QueryRoute.WRITE, AbstractSqlQueryReactor.detectQueryRoute(query, false));
	}

	@Test
	void selectIntoUsesTheWriteRoute() {
		assertEquals(AbstractSqlQueryReactor.QueryRoute.WRITE,
				AbstractSqlQueryReactor.detectQueryRoute("SELECT id INTO archived_ids FROM orders", false));
	}

	@Test
	void selectForUpdateReturnsRowsButRequiresEditAccess() {
		AbstractSqlQueryReactor.QueryRoute route = AbstractSqlQueryReactor
				.detectQueryRoute("SELECT id FROM orders FOR UPDATE", false);

		assertEquals(AbstractSqlQueryReactor.QueryRoute.LOCKING_READ, route);
		assertEquals(true, route.returnsRows);
		assertEquals(true, route.requiresEdit);
	}

	@Test
	void sqlServerSquareBracketIdentifiersCanBeEnabled() {
		assertEquals(AbstractSqlQueryReactor.QueryRoute.READ,
				AbstractSqlQueryReactor.detectQueryRoute("SELECT [Order ID] FROM [Sales Orders]", true));
	}

	@Test
	void multipleStatementsAreRejectedInsteadOfRoutedFromTheFirstStatement() {
		assertThrows(IllegalArgumentException.class,
				() -> AbstractSqlQueryReactor.detectQueryRoute("SELECT 1; DELETE FROM orders", false));
	}

	@Test
	void multipleStatementsAreParsedInExecutionOrderWithIndependentRoutes() {
		List<AbstractSqlQueryReactor.ParsedSqlStatement> statements = AbstractSqlQueryReactor
				.parseQueryStatements("SELECT id FROM orders; UPDATE orders SET status = 'open' WHERE id = 1", false);

		assertEquals(2, statements.size());
		assertEquals(AbstractSqlQueryReactor.QueryRoute.READ, statements.get(0).route);
		assertEquals(AbstractSqlQueryReactor.QueryRoute.WRITE, statements.get(1).route);
		assertEquals("SELECT id FROM orders", statements.get(0).sql);
	}

	@ParameterizedTest
	@ValueSource(strings = { "", " ", "SELECT (", "WAITFOR DELAY '00:00:20'" })
	void emptyMalformedAndUnsupportedSqlAreRejectedInsteadOfGuessed(String query) {
		assertThrows(IllegalArgumentException.class, () -> AbstractSqlQueryReactor.detectQueryRoute(query, false));
	}

	@ParameterizedTest
	@ValueSource(strings = { "CREATE INDEX IF NOT EXISTS idx_orders_id ON orders (id)",
			"CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_pair ON orders (id, status)",
			"CREATE INDEX idx_orders_status ON orders (status)" })
	void createIndexUsesTheWriteRoute(String query) {
		assertEquals(AbstractSqlQueryReactor.QueryRoute.WRITE, AbstractSqlQueryReactor.detectQueryRoute(query, false),
				() -> "for " + query);
	}

	@ParameterizedTest
	@ValueSource(strings = { "SHOW INDEX FROM orders", "SHOW DATABASES", "SHOW COLUMNS FROM orders", "DESC orders" })
	void otherMetadataStatementsAlsoUseTheReadRoute(String query) {
		// SHOW TABLES and SHOW INDEX are their own statement types rather than
		// subclasses of ShowStatement, so each has to be recognised on its own
		assertEquals(AbstractSqlQueryReactor.QueryRoute.READ, AbstractSqlQueryReactor.detectQueryRoute(query, false),
				() -> "for " + query);
	}

	@ParameterizedTest
	@ValueSource(strings = { "SELECT id FROM orders UNION SELECT id FROM archived_orders", "(SELECT id FROM orders)" })
	void setOperationsAndParenthesizedSelectsUseTheReadRoute(String query) {
		assertEquals(AbstractSqlQueryReactor.QueryRoute.READ, AbstractSqlQueryReactor.detectQueryRoute(query, false),
				() -> "for " + query);
	}

	@Test
	void aLockingReadAnywhereInASetOperationMakesTheWholeThingALockingRead() {
		assertEquals(AbstractSqlQueryReactor.QueryRoute.LOCKING_READ, AbstractSqlQueryReactor
				.detectQueryRoute("SELECT id FROM orders UNION SELECT id FROM archived_orders FOR UPDATE", false));
	}

	@ParameterizedTest
	@ValueSource(strings = { "DROP TABLE orders", "ALTER TABLE orders ADD COLUMN note VARCHAR(10)",
			"GRANT SELECT ON orders TO bob" })
	void unrecognisedAndDdlStatementsFailClosedOntoTheWriteRoute(String query) {
		// demanding edit rights for something we cannot classify is the safe default
		assertEquals(AbstractSqlQueryReactor.QueryRoute.WRITE, AbstractSqlQueryReactor.detectQueryRoute(query, false),
				() -> "for " + query);
	}
}
