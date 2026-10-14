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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.HeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;

class AutomationTableCaptureServiceUnitTests {

	@Test
	void capturesPagedFrameRowsForDurableHistory() throws Exception {
		ITableDataFrame frame = mock(ITableDataFrame.class);
		OwlTemporalEngineMeta metadata = mock(OwlTemporalEngineMeta.class);
		IRawSelectWrapper wrapper = mock(IRawSelectWrapper.class);
		when(frame.size("query_result")).thenReturn(2L);
		when(frame.getQsHeaders()).thenReturn(new String[] { "QUERY_RESULT__ID", "QUERY_RESULT__STATUS" });
		when(frame.getColumnHeaders()).thenReturn(new String[] { "ID", "STATUS" });
		when(frame.getMetaData()).thenReturn(metadata);
		when(frame.getFrameType()).thenReturn(DataFrameTypeEnum.PYTHON);
		when(metadata.getHeaderTypeAsString("QUERY_RESULT__ID")).thenReturn("INT");
		when(metadata.getHeaderTypeAsString("QUERY_RESULT__STATUS")).thenReturn("STRING");
		when(frame.query(any(SelectQueryStruct.class))).thenReturn(wrapper);
		when(wrapper.hasNext()).thenReturn(true, true, false);
		when(wrapper.next()).thenReturn(
				new HeadersDataRow(new String[] { "ID", "STATUS" }, new Object[] { 1, "ready" }),
				new HeadersDataRow(new String[] { "ID", "STATUS" }, new Object[] { 2, "done" }));
		AutomationFrameOutput.RegisteredFrame output =
				AutomationFrameOutput.register(new prerna.om.Insight(), "query_result", frame);

		AutomationTableCaptureService.TableCapture capture =
				AutomationTableCaptureService.capture(output, "query");

		assertEquals(2, capture.rowCount());
		assertEquals("[[1,\"ready\"],[2,\"done\"]]", capture.chunks().get(0).payload());
		assertEquals(output.summary(), AutomationTableCaptureService.summary(capture));
	}
}
