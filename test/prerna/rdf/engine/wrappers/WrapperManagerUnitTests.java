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
package prerna.rdf.engine.wrappers;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryOpaqueSelector;
import prerna.usertracking.UserQueryTrackingThread;
import prerna.util.Constants;
import prerna.util.JdbcTestDatabase;

class WrapperManagerUnitTests {

	private SelectQueryStruct query() {
		SelectQueryStruct query = new SelectQueryStruct();
		query.addSelector(new QueryColumnSelector("ITEMS__V", "label"));
		query.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__V", "==", "O'Brien"));
		return query;
	}

	@Test
	void deferredReadsCountAndResetTrackTemplatesOnlyWhenExecuted() throws Exception {
		try (var db = new JdbcTestDatabase(); var trackers = mockConstruction(UserQueryTrackingThread.class)) {
			db.execute("CREATE TABLE ITEMS (V VARCHAR)", "INSERT INTO ITEMS VALUES ('O''Brien')");
			when(db.engine.isBasic()).thenReturn(true);
			when(db.engine.getEngineId()).thenReturn("catalog-tracking-test");
			when(db.engine.getDatabaseZoneId()).thenReturn(ZoneOffset.UTC);
			try (var wrapper = WrapperManager.getInstance().getPreparedWrapper(db.engine, query(), true, 2)) {
				assertInstanceOf(RawPreparedRDBMSSelectWrapper.class, wrapper);
				verify(db.engine, never()).getConnection();
				assertTrue(trackers.constructed().isEmpty());
				wrapper.execute();
				assertEquals(1, wrapper.getNumRows());
				assertEquals(1, wrapper.getNumRows()); // Cached counts are not separate queries.
				assertEquals("O'Brien", wrapper.next().getValues()[0]);
				wrapper.reset();
				assertEquals(3, trackers.constructed().size());
				for (var tracker : trackers.constructed()) {
					verify(tracker).setQuery(argThat(sql -> sql.contains("?") && !sql.contains("O'Brien")));
					verify(tracker).setStartTimeNow();
					verify(tracker).setEndTimeNow();
					verify(tracker, never()).setFailed();
					verify(tracker, timeout(1000)).run();
				}
			}
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "prepare", "count" })
	void failuresAreTrackedAndReleaseReadResources(String stage) throws Exception {
		try (var db = new JdbcTestDatabase(); var trackers = mockConstruction(UserQueryTrackingThread.class)) {
			db.execute("CREATE TABLE ITEMS (V VARCHAR)");
			when(db.engine.isBasic()).thenReturn(true);
			when(db.engine.getEngineId()).thenReturn("catalog-tracking-test");
			db.manual();
			if (stage.equals("prepare")) {
				doThrow(new SQLException("prepare failed")).when(db.connection).prepareStatement(anyString());
				assertThrows(SQLException.class,
						() -> WrapperManager.getInstance().getPreparedWrapper(db.engine, query()));
			} else {
				try (var wrapper = WrapperManager.getInstance().getPreparedWrapper(db.engine, query())) {
					assertInstanceOf(RawPreparedRDBMSSelectWrapper.class, wrapper);
					doThrow(new SQLException("count failed")).when(db.connection)
							.prepareStatement(startsWith("SELECT COUNT"));
					assertThrows(IllegalArgumentException.class, wrapper::getNumRows);
				}
			}
			verify(trackers.constructed().getLast()).setFailed();
			verify(trackers.constructed().getLast()).setEndTimeNow();
			verify(db.connection, atLeastOnce()).rollback();
		}
	}

	@ParameterizedTest
	@NullSource
	@ValueSource(strings = { Constants.SECURITY_DB, Constants.USER_TRACKING_DB, "test_" + Constants.OWL_ENGINE_SUFFIX,
			"test_" + Constants.RDBMS_INSIGHTS_ENGINE_SUFFIX })
	void internalQueriesDoNotCreateTrackingLoops(String engineId) throws Exception {
		try (var db = new JdbcTestDatabase(); var trackers = mockConstruction(UserQueryTrackingThread.class)) {
			when(db.engine.getEngineId()).thenReturn(engineId);
			assertNull(WrapperManager.preparedQueryTracker(db.engine, "SELECT ?"));
			assertTrue(trackers.constructed().isEmpty());
		}
	}

	@Test
	void unsupportedStructuresFailBeforeAcquiringAConnection() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			SelectQueryStruct invalid = query();
			invalid.addSelector(new QueryOpaqueSelector());
			assertThrows(IllegalArgumentException.class,
					() -> WrapperManager.getInstance().getPreparedWrapper(db.engine, invalid));
			verify(db.engine, never()).getConnection();
		}
	}

	@Test
	void trackingCompletionFailureDoesNotDiscardASuccessfulRead() throws Exception {
		var tracker = mock(UserQueryTrackingThread.class);
		doThrow(new IllegalStateException("tracking unavailable")).when(tracker).setEndTimeNow();
		assertDoesNotThrow(() -> WrapperManager.finishPreparedQueryTracking(tracker));
		assertDoesNotThrow(() -> WrapperManager.finishPreparedQueryTracking(null));
	}
}
