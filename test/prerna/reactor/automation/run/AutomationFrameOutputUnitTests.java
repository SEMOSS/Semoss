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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.HeadersDataRow;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.PixelDataType;

/** Covers the internal Insight-owned Automation frame reference. */
public class AutomationFrameOutputUnitTests {

	@Test
	void registersExistingFrameWithoutCopyingRows() {
		ITableDataFrame frame = mock(ITableDataFrame.class);
		when(frame.size("query_result")).thenReturn(25L);
		when(frame.getQsHeaders()).thenReturn(new String[] { "QUERY_RESULT__ID", "QUERY_RESULT__STATUS" });
		when(frame.getFrameType()).thenReturn(DataFrameTypeEnum.PYTHON);
		Insight insight = new Insight();

		AutomationFrameOutput.RegisteredFrame output = AutomationFrameOutput.register(insight, "query_result", frame);

		assertEquals(Map.of("dataType", "table", "rowCount", 25L, "columnCount", 2), output.summary());
		assertSame(frame, output.frame());
		assertEquals(DataFrameTypeEnum.PYTHON.getTypeAsString(), output.backend());
		assertEquals(PixelDataType.FRAME, insight.getVarStore().get("query_result").getNounType());
		assertSame(frame, insight.getVarStore().get("query_result").getValue());
	}

	@Test
	void readsBoundedDuplicatePreservingPages() throws Exception {
		ITableDataFrame frame = mock(ITableDataFrame.class);
		IRawSelectWrapper wrapper = mock(IRawSelectWrapper.class);
		when(frame.size("query_result")).thenReturn(3L);
		when(frame.getQsHeaders()).thenReturn(new String[] { "QUERY_RESULT__VALUE" });
		when(frame.getColumnHeaders()).thenReturn(new String[] { "VALUE" });
		when(frame.getFrameType()).thenReturn(DataFrameTypeEnum.PYTHON);
		when(frame.query(any(SelectQueryStruct.class))).thenReturn(wrapper);
		when(wrapper.hasNext()).thenReturn(true, true, false);
		when(wrapper.next()).thenReturn(
				new HeadersDataRow(new String[] { "VALUE" }, new Object[] { 7 }),
				new HeadersDataRow(new String[] { "VALUE" }, new Object[] { 7 }));
		AutomationFrameOutput.RegisteredFrame output =
				AutomationFrameOutput.register(new Insight(), "query_result", frame);

		assertEquals(List.of(Map.of("VALUE", 7), Map.of("VALUE", 7)), output.page(0, 2));

		org.mockito.ArgumentCaptor<SelectQueryStruct> query =
				org.mockito.ArgumentCaptor.forClass(SelectQueryStruct.class);
		org.mockito.Mockito.verify(frame).query(query.capture());
		assertFalse(query.getValue().isDistinct());
		assertEquals(0L, query.getValue().getOffset());
		assertEquals(2L, query.getValue().getLimit());
	}
}
