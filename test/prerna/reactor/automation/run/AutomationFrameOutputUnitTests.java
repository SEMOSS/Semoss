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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PyTranslator;
import prerna.om.Insight;
import prerna.reactor.frame.convert.ConvertReactor;
import prerna.reactor.frame.py.GenerateFrameFromPyVariableReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Covers the shared Insight-owned frame registration boundary. */
public class AutomationFrameOutputUnitTests {

	@Test
	void registersExistingFrameWithActualShapeAndBackend() {
		ITableDataFrame frame = mock(ITableDataFrame.class);
		when(frame.size("query_result")).thenReturn(25L);
		when(frame.getColumnHeaders()).thenReturn(new String[] { "ID", "STATUS" });
		when(frame.getFrameType()).thenReturn(DataFrameTypeEnum.PYTHON);
		Insight insight = new Insight();
		insight.setInsightId("automation-run-1");

		AutomationFrameOutput.RegisteredFrame output = AutomationFrameOutput.register(insight, "query_result", frame);

		assertEquals(Map.of("dataType", "table", "rowCount", 25L, "columnCount", 2), output.summary());
		assertEquals(DataFrameTypeEnum.PYTHON.getTypeAsString(), output.backend());
		assertEquals(PixelDataType.FRAME, insight.getVarStore().get("query_result").getNounType());
		assertSame(frame, insight.getVarStore().get("query_result").getValue());
	}

	@Test
	void requiresRegisteredFrameIdentityRatherThanJsonShape() {
		Insight insight = new Insight();
		insight.setInsightId("automation-run-1");

		IllegalStateException unavailable = assertThrows(IllegalStateException.class,
				() -> AutomationFrameOutput.requireBackend(insight, "ordinary_json"));

		assertEquals("Automation frame output 'ordinary_json' was not registered in execution Insight "
				+ "'automation-run-1'.", unavailable.getMessage());
	}

	@Test
	void doesNotRegisterAnExistingFrameWhenDescriptionFails() {
		ITableDataFrame frame = mock(ITableDataFrame.class);
		when(frame.size("query_result")).thenThrow(new IllegalStateException("size failed"));
		Insight insight = new Insight();
		insight.setInsightId("automation-run-1");

		assertThrows(IllegalStateException.class,
				() -> AutomationFrameOutput.register(insight, "query_result", frame));
		assertNull(insight.getVarStore().get("query_result"));
	}

	@Test
	void registrationFailureRemovesTheAliasAndClosesTheCreatedFrame() {
		ITableDataFrame frame = mock(ITableDataFrame.class);
		when(frame.size("query_result")).thenThrow(new IllegalStateException("size failed"));
		Insight insight = new Insight();
		insight.setInsightId("automation-run-1");

		try (var reactors = mockConstruction(GenerateFrameFromPyVariableReactor.class, (reactor, context) -> when(
				reactor.execute()).thenAnswer(invocation -> {
					insight.getVarStore().put("query_result", new NounMetadata(frame, PixelDataType.FRAME));
					return new NounMetadata(frame, PixelDataType.FRAME);
				}))) {
			assertThrows(IllegalStateException.class,
					() -> AutomationFrameOutput.registerPythonVariable(insight, "query_result"));
		}

		assertNull(insight.getVarStore().get("query_result"));
		verify(frame).close();
	}

	@Test
	void discardsFailedPythonFrameValueFromTheLiveNamespace() {
		PyTranslator translator = mock(PyTranslator.class);

		AutomationFrameOutput.discardPythonValue(translator, "query_result", "run-1", "node-1");

		verify(translator).runScript("globals().pop(\"query_result\", None)");
	}

	@Test
	void materializesOnlyRequiredNativeBindingsForPython() {
		NativeFrame nativeFrame = mock(NativeFrame.class);
		PandasFrame pythonFrame = mock(PandasFrame.class);
		when(pythonFrame.getFrameType()).thenReturn(DataFrameTypeEnum.PYTHON);
		Insight insight = new Insight();
		insight.setInsightId("automation-run-1");
		insight.getVarStore().put("query_result", new NounMetadata(nativeFrame, PixelDataType.FRAME));
		Map<String, String> bindings = new LinkedHashMap<>(
				Map.of("query_result", DataFrameTypeEnum.NATIVE.getTypeAsString(), "unused_rows",
						DataFrameTypeEnum.NATIVE.getTypeAsString()));

		try (var reactors = mockConstruction(ConvertReactor.class, (reactor, context) -> when(reactor.execute())
				.thenAnswer(invocation -> {
					insight.getVarStore().put("query_result", new NounMetadata(pythonFrame, PixelDataType.FRAME));
					return new NounMetadata(pythonFrame, PixelDataType.FRAME);
				}))) {
			AutomationFrameOutput.materializePythonBindings(insight, bindings, Set.of("query_result"), "run-1",
					"node-1");

			assertEquals(1, reactors.constructed().size());
		}

		assertEquals(DataFrameTypeEnum.PYTHON.getTypeAsString(), bindings.get("query_result"));
		assertEquals(DataFrameTypeEnum.NATIVE.getTypeAsString(), bindings.get("unused_rows"));
		assertSame(pythonFrame, insight.getVarStore().get("query_result").getValue());
		verify(nativeFrame).close();
	}

	@Test
	void releasesOnlyIterationOwnedFramesAndPreservesParentBindings() {
		ITableDataFrame parentFrame = mock(ITableDataFrame.class);
		ITableDataFrame iterationFrame = mock(ITableDataFrame.class);
		PyTranslator translator = mock(PyTranslator.class);
		Insight insight = new Insight();
		insight.setInsightId("automation-run-1");
		insight.getVarStore().put("parent_rows", new NounMetadata(parentFrame, PixelDataType.FRAME));
		insight.getVarStore().put("lookup_rows", new NounMetadata(iterationFrame, PixelDataType.FRAME));
		Map<String, String> bindings = new LinkedHashMap<>(Map.of("parent_rows", "PY", "lookup_rows", "PY"));

		AutomationFrameOutput.releaseIterationFrames(insight, translator, Set.of(iterationFrame), bindings, "run-1",
				"loop-1", 0);

		assertSame(parentFrame, insight.getVarStore().get("parent_rows").getValue());
		assertNull(insight.getVarStore().get("lookup_rows"));
		assertEquals(Map.of("parent_rows", "PY"), bindings);
		verify(iterationFrame).close();
		verify(parentFrame, never()).close();
		verify(translator).runScript("globals().pop(\"lookup_rows\", None)");
	}
}
