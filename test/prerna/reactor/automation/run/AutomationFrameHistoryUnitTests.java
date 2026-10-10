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
package prerna.reactor.automation.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.HeadersDataRow;
import prerna.reactor.automation.AutomationConstants;
import prerna.util.JdbcTestDatabase;

class AutomationFrameHistoryUnitTests {

	@Test
	void duplicateRowsAndFinalPartialChunkRemainPageableAfterCapture() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute(
					"CREATE TABLE AUTOMATION_RUN_DATA (DATA_REFERENCE_ID VARCHAR PRIMARY KEY, RUN_ID VARCHAR, NODE_ID VARCHAR, OUTPUT_VAR_NAME VARCHAR, DATA_STATE VARCHAR, DATA_HEADERS CLOB, DATA_TYPES CLOB, DATA_ROW_COUNT BIGINT, DATA_COLUMN_COUNT INT, DATA_CONTENT_BYTES BIGINT, DATA_CREATED_AT TIMESTAMP, DATA_AVAILABLE_AT TIMESTAMP)",
					"CREATE TABLE AUTOMATION_RUN_DATA_CHUNKS (DATA_REFERENCE_ID VARCHAR, DATA_CHUNK_INDEX INT, DATA_ROW_OFFSET BIGINT, DATA_CHUNK_ROW_COUNT INT, DATA_ROWS CLOB, PRIMARY KEY (DATA_REFERENCE_ID, DATA_CHUNK_INDEX))");

			List<Object[]> values = new ArrayList<>();
			values.add(new Object[] { 7 });
			values.add(new Object[] { 7 });
			for (int index = 2; index < AutomationConstants.RUN_DATA_CHUNK_SIZE; index++) {
				values.add(new Object[] { index });
			}
			values.add(new Object[] { null });
			values.add(new Object[] { "" });

			IRawSelectWrapper wrapper = mock(IRawSelectWrapper.class);
			when(wrapper.getHeaders()).thenReturn(new String[] { "VALUE" });
			when(wrapper.getTypes()).thenReturn(new SemossDataType[] { SemossDataType.STRING });
			AtomicInteger cursor = new AtomicInteger();
			when(wrapper.hasNext()).thenAnswer(invocation -> cursor.get() < values.size());
			when(wrapper.next()).thenAnswer(invocation ->
					new HeadersDataRow(new String[] { "VALUE" }, values.get(cursor.getAndIncrement())));

			AutomationFrameHistory.Snapshot snapshot = AutomationFrameHistory.captureWrapper("run", "node", "result",
					wrapper);
			assertEquals(1002, snapshot.rowCount());
			assertEquals(2, db.count("AUTOMATION_RUN_DATA_CHUNKS"));
			assertEquals(AutomationConstants.DATA_STATE_AVAILABLE,
					db.value("SELECT DATA_STATE FROM AUTOMATION_RUN_DATA"));

			AutomationFrameHistory.Page first = AutomationFrameHistory.page("run", "node", 0, 2);
			assertEquals(2, first.rows().size());
			assertEquals(7, ((Number) first.rows().get(0).get(0)).intValue());
			assertEquals(7, ((Number) first.rows().get(1).get(0)).intValue());

			AutomationFrameHistory.Page last = AutomationFrameHistory.page("run", "node", 1000, 2);
			assertEquals(2, last.rows().size());
			assertNull(last.rows().get(0).get(0));
			assertEquals("", last.rows().get(1).get(0));
			assertTrue(AutomationFrameHistory.findAvailableByRun("run").containsKey("node"));
		}
	}
}
