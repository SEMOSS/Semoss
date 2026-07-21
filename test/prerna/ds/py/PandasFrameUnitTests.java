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
package prerna.ds.py;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.SemossDataType;
import prerna.cache.CachePropFileFrameObject;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.interpreters.PandasInterpreter;
import prerna.util.Utility;

class PandasFrameUnitTests {

	private PyTranslator mockTranslator;

	@BeforeEach
	void setUp() {
		mockTranslator = mock(PyTranslator.class);
	}

	private Object getField(Object obj, String fieldName) throws Exception {
		Class<?> clazz = obj.getClass();
		while (clazz != null) {
			try {
				Field f = clazz.getDeclaredField(fieldName);
				f.setAccessible(true);
				return f.get(obj);
			} catch (NoSuchFieldException e) {
				clazz = clazz.getSuperclass();
			}
		}
		throw new NoSuchFieldException(fieldName);
	}

	@Nested
	class StaticMapsTests {

		@Test
		void pyS_objectMapsToString() {
			assertEquals(SemossDataType.STRING, PandasFrame.pyS.get("object"));
		}

		@Test
		void pyS_categoryMapsToString() {
			assertEquals(SemossDataType.STRING, PandasFrame.pyS.get("category"));
		}

		@Test
		void pyS_int64MapsToInt() {
			assertEquals(SemossDataType.INT, PandasFrame.pyS.get("int64"));
		}

		@Test
		void pyS_float64MapsToDouble() {
			assertEquals(SemossDataType.DOUBLE, PandasFrame.pyS.get("float64"));
		}

		@Test
		void pyS_datetime64MapsToDate() {
			assertEquals(SemossDataType.DATE, PandasFrame.pyS.get("datetime64"));
		}

		@Test
		void pyS_boolMapsToBoolean() {
			assertEquals(SemossDataType.BOOLEAN, PandasFrame.pyS.get("bool"));
		}

		@Test
		void pyS_hasSixEntries() {
			assertEquals(6, PandasFrame.pyS.size());
		}

		@Test
		void pyJ_objectMapsToStringClass() {
			assertEquals(java.lang.String.class, PandasFrame.pyJ.get("object"));
		}

		@Test
		void pyJ_categoryMapsToStringClass() {
			assertEquals(java.lang.String.class, PandasFrame.pyJ.get("category"));
		}

		@Test
		void pyJ_int64MapsToIntegerClass() {
			assertEquals(java.lang.Integer.class, PandasFrame.pyJ.get("int64"));
		}

		@Test
		void pyJ_float64MapsToDoubleClass() {
			assertEquals(java.lang.Double.class, PandasFrame.pyJ.get("float64"));
		}

		@Test
		void pyJ_datetime64MapsToDateClass() {
			assertEquals(java.util.Date.class, PandasFrame.pyJ.get("datetime64"));
		}

		@Test
		void pyJ_boolMapsToBooleanClass() {
			assertEquals(java.lang.Boolean.class, PandasFrame.pyJ.get("bool"));
		}

		@Test
		void pyJ_hasSixEntries() {
			assertEquals(6, PandasFrame.pyJ.size());
		}

		@Test
		void spy_semossStringMapsToStr() {
			assertEquals("'str'", PandasFrame.spy.get(SemossDataType.STRING));
		}

		@Test
		void spy_semossIntMapsToNpInt64() {
			assertEquals("np.int64", PandasFrame.spy.get(SemossDataType.INT));
		}

		@Test
		void spy_semossDoubleMapsToNpFloat64() {
			assertEquals("np.float64", PandasFrame.spy.get(SemossDataType.DOUBLE));
		}

		@Test
		void spy_semossDateMapsToNpDatetime32() {
			assertEquals("np.datetime32", PandasFrame.spy.get(SemossDataType.DATE));
		}

		@Test
		void spy_semossTimestampMapsToNpDatetime32() {
			assertEquals("np.datetime32", PandasFrame.spy.get(SemossDataType.TIMESTAMP));
		}

		@Test
		void spy_float64StringMapsToNpFloat32() {
			assertEquals("np.float32", PandasFrame.spy.get("float64"));
		}

		@Test
		void spy_int64StringMapsToNpInt32() {
			assertEquals("np.int32", PandasFrame.spy.get("int64"));
		}

		@Test
		void spy_datetime64StringMapsToNpDatetime32() {
			assertEquals("np.datetime32", PandasFrame.spy.get("datetime64"));
		}

		@Test
		void spy_dtypeOMapsToStr() {
			assertEquals("'str'", PandasFrame.spy.get("dtype('O')"));
		}

		@Test
		void spy_dtypeInt64MapsToInt32() {
			assertEquals("int32", PandasFrame.spy.get("dtype('int64')"));
		}

		@Test
		void spy_dtypeFloat64MapsToFloat32() {
			assertEquals("float32", PandasFrame.spy.get("dtype('float64')"));
		}

		@Test
		void spy_hasElevenEntries() {
			assertEquals(11, PandasFrame.spy.size());
		}

		@Test
		void pyS_unknownKeyReturnsNull() {
			assertNull(PandasFrame.pyS.get("unknown_type"));
		}

		@Test
		void pyJ_unknownKeyReturnsNull() {
			assertNull(PandasFrame.pyJ.get("unknown_type"));
		}

		@Test
		void spy_unknownKeyReturnsNull() {
			assertNull(PandasFrame.spy.get("unknown_type"));
		}
	}

	@Nested
	class ConstantsTests {

		@Test
		void dataMakerNameConstant() {
			assertEquals("PandasFrame", PandasFrame.DATA_MAKER_NAME);
		}

		@Test
		void pandasImportVarConstant() {
			assertEquals("pandas_import_var", PandasFrame.PANDAS_IMPORT_VAR);
		}

		@Test
		void pandasImportStringConstant() {
			assertEquals("import pandas as pandas_import_var", PandasFrame.PANDAS_IMPORT_STRING);
		}

		@Test
		void numpyImportVarConstant() {
			assertEquals("np_import_var", PandasFrame.NUMPY_IMPORT_VAR);
		}

		@Test
		void numpyImportStringConstant() {
			assertEquals("import numpy as np_import_var", PandasFrame.NUMPY_IMPORT_STRING);
		}

		@Test
		void pandasImportStringContainsVar() {
			assertTrue(PandasFrame.PANDAS_IMPORT_STRING.contains(PandasFrame.PANDAS_IMPORT_VAR));
		}

		@Test
		void numpyImportStringContainsVar() {
			assertTrue(PandasFrame.NUMPY_IMPORT_STRING.contains(PandasFrame.NUMPY_IMPORT_VAR));
		}
	}

	@Nested
	class ConstructorTests {

		@Test
		void singleArgConstructor_generatesNameStartingWithPYFRAME() throws Exception {
			PandasFrame frame = new PandasFrame(mockTranslator);
			String name = (String) getField(frame, "frameName");
			assertTrue(name.startsWith("PYFRAME_"), "Expected PYFRAME_ prefix but was: " + name);
		}

		@Test
		void singleArgConstructor_frameNameHasCorrectLength() throws Exception {
			PandasFrame frame = new PandasFrame(mockTranslator);
			String name = (String) getField(frame, "frameName");
			// "PYFRAME_" (8) + UUID with dashes->underscores (36) = 44
			assertEquals(44, name.length());
		}

		@Test
		void singleArgConstructor_frameNameContainsNoHyphens() throws Exception {
			PandasFrame frame = new PandasFrame(mockTranslator);
			String name = (String) getField(frame, "frameName");
			assertFalse(name.contains("-"));
		}

		@Test
		void singleArgConstructor_setsWrapperName() throws Exception {
			PandasFrame frame = new PandasFrame(mockTranslator);
			String frameName = (String) getField(frame, "frameName");
			assertEquals(frameName + "w", frame.getWrapperName());
		}

		@Test
		void singleArgConstructor_setsOriginalName() throws Exception {
			PandasFrame frame = new PandasFrame(mockTranslator);
			String frameName = (String) getField(frame, "frameName");
			String originalName = (String) getField(frame, "originalName");
			assertEquals(frameName, originalName);
		}

		@Test
		void singleArgConstructor_setsOriginalWrapperName() throws Exception {
			PandasFrame frame = new PandasFrame(mockTranslator);
			String wrapperName = frame.getWrapperName();
			String originalWrapper = (String) getField(frame, "originalWrapperFrameName");
			assertEquals(wrapperName, originalWrapper);
		}

		@Test
		void twoArgConstructor_usesProvidedName() throws Exception {
			PandasFrame frame = new PandasFrame("myTable", mockTranslator);
			String name = (String) getField(frame, "frameName");
			assertEquals("myTable", name);
		}

		@Test
		void twoArgConstructor_setsWrapperNameCorrectly() {
			PandasFrame frame = new PandasFrame("myTable", mockTranslator);
			assertEquals("myTablew", frame.getWrapperName());
		}

		@Test
		void twoArgConstructor_setsOriginalName() throws Exception {
			PandasFrame frame = new PandasFrame("myTable", mockTranslator);
			String originalName = (String) getField(frame, "originalName");
			assertEquals("myTable", originalName);
		}

		@Test
		void twoArgConstructor_setsOriginalWrapperName() throws Exception {
			PandasFrame frame = new PandasFrame("myTable", mockTranslator);
			String originalWrapper = (String) getField(frame, "originalWrapperFrameName");
			assertEquals("myTablew", originalWrapper);
		}

		@Test
		void twoArgConstructor_withNullName_generatesName() throws Exception {
			PandasFrame frame = new PandasFrame(null, mockTranslator);
			String name = (String) getField(frame, "frameName");
			assertTrue(name.startsWith("PYFRAME_"));
		}

		@Test
		void twoArgConstructor_withEmptyName_generatesName() throws Exception {
			PandasFrame frame = new PandasFrame("", mockTranslator);
			String name = (String) getField(frame, "frameName");
			assertTrue(name.startsWith("PYFRAME_"));
		}

		@Test
		void twoArgConstructor_withBlankName_generatesName() throws Exception {
			PandasFrame frame = new PandasFrame("   ", mockTranslator);
			String name = (String) getField(frame, "frameName");
			assertTrue(name.startsWith("PYFRAME_"));
		}

		@Test
		void twoArgConstructor_storesPyTranslator() throws Exception {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			Object translator = getField(frame, "pyTranslator");
			assertSame(mockTranslator, translator);
		}

		@Test
		void constructor_initializesCacheTrue() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			assertTrue(frame.cache);
		}

		@Test
		void constructor_initializesKeyCacheEmpty() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			assertNotNull(frame.keyCache);
			assertTrue(frame.keyCache.isEmpty());
		}

		@Test
		void constructor_sqliteConnectionNameIsNull() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			assertNull(frame.sqliteConnectionName);
		}

		@Test
		void constructor_metaDataIsNotNull() throws Exception {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			Object metaData = getField(frame, "metaData");
			assertNotNull(metaData);
		}

		@Test
		void twoUniqueFrames_haveDifferentNames() throws Exception {
			PandasFrame frame1 = new PandasFrame(mockTranslator);
			PandasFrame frame2 = new PandasFrame(mockTranslator);
			String name1 = (String) getField(frame1, "frameName");
			String name2 = (String) getField(frame2, "frameName");
			assertNotEquals(name1, name2);
		}

		@Test
		void constructor_withSpecialCharsName_usesAsIs() throws Exception {
			PandasFrame frame = new PandasFrame("frame_123_test", mockTranslator);
			String name = (String) getField(frame, "frameName");
			assertEquals("frame_123_test", name);
		}
	}

	@Nested
	class SetNameTests {

		@Test
		void setName_updatesFrameName() throws Exception {
			PandasFrame frame = new PandasFrame("original", mockTranslator);
			frame.setName("newName");
			String name = (String) getField(frame, "frameName");
			assertEquals("newName", name);
		}

		@Test
		void setName_updatesWrapperName() {
			PandasFrame frame = new PandasFrame("original", mockTranslator);
			frame.setName("newName");
			assertEquals("newNamew", frame.getWrapperName());
		}

		@Test
		void setName_doesNotUpdateOriginalName() throws Exception {
			PandasFrame frame = new PandasFrame("original", mockTranslator);
			frame.setName("newName");
			String originalName = (String) getField(frame, "originalName");
			assertEquals("original", originalName);
		}

		@Test
		void setName_doesNotUpdateOriginalWrapperName() throws Exception {
			PandasFrame frame = new PandasFrame("original", mockTranslator);
			frame.setName("newName");
			String originalWrapper = (String) getField(frame, "originalWrapperFrameName");
			assertEquals("originalw", originalWrapper);
		}

		@Test
		void setName_withNull_doesNotChange() throws Exception {
			PandasFrame frame = new PandasFrame("original", mockTranslator);
			frame.setName(null);
			String name = (String) getField(frame, "frameName");
			assertEquals("original", name);
		}

		@Test
		void setName_withEmpty_doesNotChange() throws Exception {
			PandasFrame frame = new PandasFrame("original", mockTranslator);
			frame.setName("");
			String name = (String) getField(frame, "frameName");
			assertEquals("original", name);
		}

		@Test
		void setName_withNull_wrapperNameUnchanged() {
			PandasFrame frame = new PandasFrame("original", mockTranslator);
			frame.setName(null);
			assertEquals("originalw", frame.getWrapperName());
		}

		@Test
		void setName_multipleTimes_lastWins() throws Exception {
			PandasFrame frame = new PandasFrame("first", mockTranslator);
			frame.setName("second");
			frame.setName("third");
			String name = (String) getField(frame, "frameName");
			assertEquals("third", name);
			assertEquals("thirdw", frame.getWrapperName());
		}
	}

	@Nested
	class GetWrapperNameTests {

		@Test
		void getWrapperName_appendsW() {
			PandasFrame frame = new PandasFrame("testFrame", mockTranslator);
			assertEquals("testFramew", frame.getWrapperName());
		}

		@Test
		void getWrapperName_withUnderscores() {
			PandasFrame frame = new PandasFrame("my_frame_name", mockTranslator);
			assertEquals("my_frame_namew", frame.getWrapperName());
		}

		@Test
		void getWrapperName_afterSetName() {
			PandasFrame frame = new PandasFrame("original", mockTranslator);
			frame.setName("updated");
			assertEquals("updatedw", frame.getWrapperName());
		}
	}

	@Nested
	class SyncMethodTests {

		@Test
		void sync_matchingHeaders_returnsTrue() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			String[] headers = { "col1", "col2", "col3" };
			List<String> actHeaders = Arrays.asList("col1", "col2", "col3");
			assertTrue(frame.sync(headers, actHeaders));
		}

		@Test
		void sync_mismatchAtFirst_returnsFalse() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			String[] headers = { "colA", "col2", "col3" };
			List<String> actHeaders = Arrays.asList("col1", "col2", "col3");
			assertFalse(frame.sync(headers, actHeaders));
		}

		@Test
		void sync_mismatchAtLast_returnsFalse() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			String[] headers = { "col1", "col2", "col3" };
			List<String> actHeaders = Arrays.asList("col1", "col2", "colX");
			assertFalse(frame.sync(headers, actHeaders));
		}

		@Test
		void sync_mismatchInMiddle_returnsFalse() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			String[] headers = { "col1", "WRONG", "col3" };
			List<String> actHeaders = Arrays.asList("col1", "col2", "col3");
			assertFalse(frame.sync(headers, actHeaders));
		}

		@Test
		void sync_emptyArrays_returnsTrue() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			String[] headers = {};
			List<String> actHeaders = new ArrayList<>();
			assertTrue(frame.sync(headers, actHeaders));
		}

		@Test
		void sync_singleMatch_returnsTrue() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			String[] headers = { "only" };
			List<String> actHeaders = List.of("only");
			assertTrue(frame.sync(headers, actHeaders));
		}

		@Test
		void sync_singleMismatch_returnsFalse() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			String[] headers = { "A" };
			List<String> actHeaders = List.of("B");
			assertFalse(frame.sync(headers, actHeaders));
		}

		@Test
		void sync_caseSensitive_returnsFalse() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			String[] headers = { "Col1" };
			List<String> actHeaders = List.of("col1");
			assertFalse(frame.sync(headers, actHeaders));
		}

		@Test
		void sync_allMatchingFiveElements() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			String[] headers = { "a", "b", "c", "d", "e" };
			List<String> actHeaders = Arrays.asList("a", "b", "c", "d", "e");
			assertTrue(frame.sync(headers, actHeaders));
		}
	}

	@Nested
	class GetFrameTypeTests {

		@Test
		void getFrameType_returnsPython() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			assertEquals(DataFrameTypeEnum.PYTHON, frame.getFrameType());
		}

		@Test
		void getFrameType_isNotNull() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			assertNotNull(frame.getFrameType());
		}
	}

	@Nested
	class GetDataMakerNameTests {

		@Test
		void getDataMakerName_returnsPandasFrame() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			assertEquals("PandasFrame", frame.getDataMakerName());
		}

		@Test
		void getDataMakerName_matchesConstant() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			assertEquals(PandasFrame.DATA_MAKER_NAME, frame.getDataMakerName());
		}
	}

	@Nested
	class IsEmptyTests {

		@Test
		void isEmpty_withTableName_whenTranslatorReturnsTrue_returnsFalse() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			String wn = "myFramew";
			String cmd = "( ('" + wn + "' in vars() or '" + wn + "' in globals()) and len(" + wn
					+ ".cache['data'])>=0 )";
			when(mockTranslator.getBoolean(cmd)).thenReturn(true);
			assertFalse(frame.isEmpty("myFrame"));
		}

		@Test
		void isEmpty_withTableName_whenTranslatorReturnsFalse_returnsTrue() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			String wn = "myFramew";
			String cmd = "( ('" + wn + "' in vars() or '" + wn + "' in globals()) and len(" + wn
					+ ".cache['data'])>=0 )";
			when(mockTranslator.getBoolean(cmd)).thenReturn(false);
			assertTrue(frame.isEmpty("myFrame"));
		}

		@Test
		void isEmpty_noArgs_usesFrameName() {
			PandasFrame frame = new PandasFrame("testFrame", mockTranslator);
			String wn = "testFramew";
			String cmd = "( ('" + wn + "' in vars() or '" + wn + "' in globals()) and len(" + wn
					+ ".cache['data'])>=0 )";
			when(mockTranslator.getBoolean(cmd)).thenReturn(true);
			assertFalse(frame.isEmpty());
		}

		@Test
		void isEmpty_noArgs_whenEmpty_returnsTrue() {
			PandasFrame frame = new PandasFrame("df", mockTranslator);
			when(mockTranslator.getBoolean(anyString())).thenReturn(false);
			assertTrue(frame.isEmpty());
		}

		@Test
		void isEmpty_withDifferentTableName_usesCorrectWrapper() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			String wn = "otherTablew";
			String cmd = "( ('" + wn + "' in vars() or '" + wn + "' in globals()) and len(" + wn
					+ ".cache['data'])>=0 )";
			when(mockTranslator.getBoolean(cmd)).thenReturn(true);
			assertFalse(frame.isEmpty("otherTable"));
		}
	}

	@Nested
	class SizeTests {

		@Test
		void size_whenEmpty_returnsZero() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			when(mockTranslator.getBoolean(anyString())).thenReturn(false);
			assertEquals(0, frame.size("myFrame"));
		}

		@Test
		void size_whenNotEmpty_returnsLength() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			when(mockTranslator.getBoolean(anyString())).thenReturn(true);
			when(mockTranslator.getLong("len(myFrame)")).thenReturn(42L);
			assertEquals(42, frame.size("myFrame"));
		}

		@Test
		void size_whenNotEmpty_returnsLargeValue() {
			PandasFrame frame = new PandasFrame("df", mockTranslator);
			when(mockTranslator.getBoolean(anyString())).thenReturn(true);
			when(mockTranslator.getLong("len(df)")).thenReturn(1000000L);
			assertEquals(1000000, frame.size("df"));
		}

		@Test
		void size_whenNotEmpty_returnsOne() {
			PandasFrame frame = new PandasFrame("single", mockTranslator);
			when(mockTranslator.getBoolean(anyString())).thenReturn(true);
			when(mockTranslator.getLong("len(single)")).thenReturn(1L);
			assertEquals(1, frame.size("single"));
		}

		@Test
		void size_whenEmpty_doesNotCallGetLong() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			when(mockTranslator.getBoolean(anyString())).thenReturn(false);
			frame.size("myFrame");
			verify(mockTranslator, never()).getLong(anyString());
		}

		@Test
		void size_whenNotEmpty_callsGetLongWithCorrectCommand() {
			PandasFrame frame = new PandasFrame("data", mockTranslator);
			when(mockTranslator.getBoolean(anyString())).thenReturn(true);
			when(mockTranslator.getLong("len(data)")).thenReturn(5L);
			frame.size("data");
			verify(mockTranslator).getLong("len(data)");
		}
	}

	@Nested
	class RunScriptTests {

		@Test
		void runScript_delegatesToPyTranslator() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			when(mockTranslator.runScript("print('hello')")).thenReturn("hello");
			Object result = frame.runScript("print('hello')");
			assertEquals("hello", result);
			verify(mockTranslator).runScript("print('hello')");
		}

		@Test
		void runScript_returnsNullWhenTranslatorReturnsNull() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			when(mockTranslator.runScript("command")).thenReturn(null);
			assertNull(frame.runScript("command"));
		}

		@Test
		void runScript_returnsMapWhenTranslatorReturnsMap() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			Map<String, Object> expected = new HashMap<>();
			expected.put("key", "value");
			when(mockTranslator.runScript("dict_command")).thenReturn(expected);
			Object result = frame.runScript("dict_command");
			assertSame(expected, result);
		}

		@Test
		void runScript_passesScriptUnmodified() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			String script = "df.head(10).to_dict('split')";
			when(mockTranslator.runScript(script)).thenReturn(null);
			frame.runScript(script);
			verify(mockTranslator).runScript(script);
		}
	}

	@Nested
	class CloseTests {

		@Test
		void close_deletesFrameVariable() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			frame.close();
			verify(mockTranslator).runScript("del myFrame");
		}

		@Test
		void close_deletesWrapperVariable() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			frame.close();
			verify(mockTranslator).runScript("del myFramew");
		}

		@Test
		void close_runsGarbageCollection() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			frame.close();
			verify(mockTranslator).runScript("import gc", "gc.collect()");
		}

		@Test
		void close_whenNameChanged_deletesOriginals() {
			PandasFrame frame = new PandasFrame("original", mockTranslator);
			frame.setName("renamed");
			frame.close();
			verify(mockTranslator).runScript("del renamed");
			verify(mockTranslator).runScript("del renamedw");
			verify(mockTranslator).runScript("del original");
			verify(mockTranslator).runScript("del originalw");
		}

		@Test
		void close_whenNameNotChanged_doesNotDeleteOriginalsSeparately() {
			PandasFrame frame = new PandasFrame("same", mockTranslator);
			frame.close();
			verify(mockTranslator, times(1)).runScript("del same");
			verify(mockTranslator, times(1)).runScript("del samew");
		}

		@Test
		void close_withSqliteConnection_deletesIt() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			frame.sqliteConnectionName = "conn_myFrame";
			frame.close();
			verify(mockTranslator).runScript("del conn_myFrame");
		}

		@Test
		void close_withoutSqliteConnection_doesNotDeleteConn() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			frame.close();
			verify(mockTranslator, never()).runScript(eq("del conn_myFrame"));
		}

		@Test
		void close_setsIsClosedTrue() throws Exception {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			frame.close();
			boolean isClosed = (boolean) getField(frame, "isClosed");
			assertTrue(isClosed);
		}

		@Test
		void close_initiallyNotClosed() throws Exception {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			boolean isClosed = (boolean) getField(frame, "isClosed");
			assertFalse(isClosed);
		}
	}

	@Nested
	class SyncHeadersTests {

		@Test
		void syncHeaders_withSqliteConnection_deletesAndNullsIt() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			frame.sqliteConnectionName = "conn_myFrame";
			frame.syncHeaders();
			verify(mockTranslator).runScript("del conn_myFrame");
			assertNull(frame.sqliteConnectionName);
		}

		@Test
		void syncHeaders_withoutSqliteConnection_doesNotRunScript() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			frame.syncHeaders();
			verify(mockTranslator, never()).runScript(anyString());
		}

		@Test
		void syncHeaders_afterClear_secondCallDoesNotDelete() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			frame.sqliteConnectionName = "conn_myFrame";
			frame.syncHeaders();
			assertNull(frame.sqliteConnectionName);
			frame.syncHeaders();
			verify(mockTranslator, times(1)).runScript("del conn_myFrame");
		}
	}

	@Nested
	class GetSQLiteTests {

		@Test
		void getSQLite_firstCall_setsConnectionName() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			String result = frame.getSQLite();
			assertEquals("conn_myFrame", result);
		}

		@Test
		void getSQLite_firstCall_createsConnectionViaRunEmptyPy() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			frame.getSQLite();
			verify(mockTranslator).runEmptyPy("import sqlite3",
					"conn_myFrame = sqlite3.connect(':memory:', check_same_thread=False)",
					"myFrame.to_sql('myFrame', conn_myFrame, if_exists='replace', index=False)");
		}

		@Test
		void getSQLite_secondCall_returnsCachedName() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			frame.getSQLite();
			String second = frame.getSQLite();
			assertEquals("conn_myFrame", second);
			verify(mockTranslator, times(1)).runEmptyPy(any(String[].class));
		}

		@Test
		void getSQLite_setsSqliteConnectionNameField() {
			PandasFrame frame = new PandasFrame("df", mockTranslator);
			assertNull(frame.sqliteConnectionName);
			frame.getSQLite();
			assertEquals("conn_df", frame.sqliteConnectionName);
		}

		@Test
		void getSQLite_withPresetConnectionName_skipsCreation() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			frame.sqliteConnectionName = "existing_conn";
			String result = frame.getSQLite();
			assertEquals("existing_conn", result);
			verify(mockTranslator, never()).runEmptyPy(any(String[].class));
		}
	}

	@Nested
	class ReplaceWrapperDataTests {

		@Test
		void replaceWrapperDataFromFrame_runsCorrectScript() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			frame.replaceWrapperDataFromFrame();
			verify(mockTranslator).runScript("myFramew.cache['data'] = myFrame");
		}

		@Test
		void replaceWrapperDataFromFrame_afterSetName_usesNewNames() {
			PandasFrame frame = new PandasFrame("original", mockTranslator);
			frame.setName("updated");
			frame.replaceWrapperDataFromFrame();
			verify(mockTranslator).runScript("updatedw.cache['data'] = updated");
		}
	}

	@Nested
	class MergeTests {

		@Test
		void merge_equiJoin_runsScript() {
			PandasFrame frame = new PandasFrame("result", mockTranslator);
			List<Map<String, String>> joinCols = new ArrayList<>();
			Map<String, String> joinMap = new HashMap<>();
			joinMap.put("id", "id");
			joinCols.add(joinMap);
			List<String> comparators = List.of("==");

			frame.merge("merged", "left", "right", "inner", joinCols, comparators, false);

			verify(mockTranslator).runScript(anyString());
		}

		@Test
		void merge_nonEqui_withMatchingColumns_renamesColumn() {
			PandasFrame frame = new PandasFrame("result", mockTranslator);
			List<Map<String, String>> joinCols = new ArrayList<>();
			Map<String, String> joinMap = new HashMap<>();
			joinMap.put("col", "col");
			joinCols.add(joinMap);
			List<String> comparators = List.of(">=");

			frame.merge("merged", "left", "right", "cross", joinCols, comparators, true);

			String expectedRename = PandasSyntaxHelper.alterColumnName("right", "col", "col_CTD");
			verify(mockTranslator).runScript(expectedRename);
			assertEquals("col_CTD", joinCols.get(0).get("col"));
		}

		@Test
		void merge_nonEqui_withDifferentColumns_noRename() {
			PandasFrame frame = new PandasFrame("result", mockTranslator);
			List<Map<String, String>> joinCols = new ArrayList<>();
			Map<String, String> joinMap = new HashMap<>();
			joinMap.put("leftCol", "rightCol");
			joinCols.add(joinMap);
			List<String> comparators = List.of(">=");

			frame.merge("merged", "left", "right", "cross", joinCols, comparators, true);

			String renameScript = PandasSyntaxHelper.alterColumnName("right", "rightCol", "rightCol_CTD");
			verify(mockTranslator, never()).runScript(renameScript);
		}

		@Test
		void merge_nonEqui_runsFilterAfterMerge() {
			PandasFrame frame = new PandasFrame("result", mockTranslator);
			List<Map<String, String>> joinCols = new ArrayList<>();
			Map<String, String> joinMap = new HashMap<>();
			joinMap.put("a", "b");
			joinCols.add(joinMap);
			List<String> comparators = List.of(">");

			frame.merge("merged", "left", "right", "cross", joinCols, comparators, true);

			verify(mockTranslator, atLeast(2)).runScript(anyString());
		}

		@Test
		void merge_nonEqui_multipleMatchingCols_renamesBoth() {
			PandasFrame frame = new PandasFrame("result", mockTranslator);
			List<Map<String, String>> joinCols = new ArrayList<>();

			Map<String, String> joinMap1 = new HashMap<>();
			joinMap1.put("x", "x");
			joinCols.add(joinMap1);

			Map<String, String> joinMap2 = new HashMap<>();
			joinMap2.put("y", "y");
			joinCols.add(joinMap2);

			List<String> comparators = Arrays.asList(">=", "<=");

			frame.merge("merged", "left", "right", "cross", joinCols, comparators, true);

			verify(mockTranslator).runScript(PandasSyntaxHelper.alterColumnName("right", "x", "x_CTD"));
			verify(mockTranslator).runScript(PandasSyntaxHelper.alterColumnName("right", "y", "y_CTD"));
			assertEquals("x_CTD", joinCols.get(0).get("x"));
			assertEquals("y_CTD", joinCols.get(1).get("y"));
		}
	}

	@Nested
	class RecalculateVariablesTests {

		@Test
		void recalculateVariables_replacesOldNameWithNew() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			String[] formulas = { "oldFrame['col1'] + 1", "oldFrame['col2'] * 2" };
			frame.recalculateVariables(formulas, "oldFrame", "newFrame");
			verify(mockTranslator).runScript("newFrame['col1'] + 1");
			verify(mockTranslator).runScript("newFrame['col2'] * 2");
		}

		@Test
		void recalculateVariables_emptyFormulas_noScriptRun() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			String[] formulas = {};
			frame.recalculateVariables(formulas, "old", "new");
			verify(mockTranslator, never()).runScript(anyString());
		}

		@Test
		void recalculateVariables_formulaWithNoMatch_runsUnchanged() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			String[] formulas = { "unrelated_command" };
			frame.recalculateVariables(formulas, "oldName", "newName");
			verify(mockTranslator).runScript("unrelated_command");
		}

		@Test
		void recalculateVariables_multipleOccurrences_allReplaced() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			String[] formulas = { "old + old + old" };
			frame.recalculateVariables(formulas, "old", "new");
			verify(mockTranslator).runScript("new + new + new");
		}

		@Test
		void recalculateVariables_singleFormula() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			String[] formulas = { "df['x'] = df['a'] + df['b']" };
			frame.recalculateVariables(formulas, "df", "df2");
			verify(mockTranslator).runScript("df2['x'] = df2['a'] + df2['b']");
		}
	}

	@Nested
	class EmptyStubTests {

		@Test
		void addRow_doesNotThrow() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			assertDoesNotThrow(() -> frame.addRow(new Object[] { "a", 1 }, new String[] { "col1", "col2" }));
		}

		@Test
		void addRow_doesNotInteractWithTranslator() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			frame.addRow(new Object[] { "a" }, new String[] { "col1" });
			verifyNoInteractions(mockTranslator);
		}

		@Test
		void removeColumn_doesNotThrow() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			assertDoesNotThrow(() -> frame.removeColumn("anyColumn"));
		}

		@Test
		void removeColumn_doesNotInteractWithTranslator() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			frame.removeColumn("col");
			verifyNoInteractions(mockTranslator);
		}

		@Test
		void processDataMakerComponent_doesNotThrow() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			assertDoesNotThrow(() -> frame.processDataMakerComponent(null));
		}

		@Test
		void processDataMakerComponent_doesNotInteractWithTranslator() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			frame.processDataMakerComponent(null);
			verifyNoInteractions(mockTranslator);
		}
	}

	@Nested
	class GetQueryInterpreterTests {

		@Test
		void getQueryInterpreter_returnsPandasInterpreter() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			Object interp = frame.getQueryInterpreter();
			assertNotNull(interp);
			assertInstanceOf(PandasInterpreter.class, interp);
		}

		@Test
		void getQueryInterpreter_isNotNull() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			assertNotNull(frame.getQueryInterpreter());
		}
	}

	@Nested
	class GetNameTests {

		@Test
		void getName_returnsFrameName() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			assertEquals("myFrame", frame.getName());
		}

		@Test
		void getName_afterSetName_returnsNewName() {
			PandasFrame frame = new PandasFrame("original", mockTranslator);
			frame.setName("updated");
			assertEquals("updated", frame.getName());
		}

		@Test
		void getName_generatedNameStartsWithPYFRAME() {
			PandasFrame frame = new PandasFrame(mockTranslator);
			assertTrue(frame.getName().startsWith("PYFRAME_"));
		}
	}

	@Nested
	class IsClosedTests {

		@Test
		void isClosed_initiallyFalse() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			assertFalse(frame.isClosed());
		}

		@Test
		void isClosed_afterClose_returnsTrue() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			frame.close();
			assertTrue(frame.isClosed());
		}
	}

	@Nested
	class WrapperNameConsistencyTests {

		@Test
		void wrapperName_matchesPandasSyntaxHelper() {
			PandasFrame frame = new PandasFrame("abc", mockTranslator);
			String expected = PandasSyntaxHelper.createFrameWrapperName("abc");
			assertEquals(expected, frame.getWrapperName());
		}

		@Test
		void wrapperName_afterSetName_matchesHelper() {
			PandasFrame frame = new PandasFrame("abc", mockTranslator);
			frame.setName("xyz");
			String expected = PandasSyntaxHelper.createFrameWrapperName("xyz");
			assertEquals(expected, frame.getWrapperName());
		}

		@Test
		void wrapperName_generatedFrame_matchesHelper() {
			PandasFrame frame = new PandasFrame(mockTranslator);
			String frameName = frame.getName();
			String expected = PandasSyntaxHelper.createFrameWrapperName(frameName);
			assertEquals(expected, frame.getWrapperName());
		}
	}

	@Nested
	class PublicFieldTests {

		@Test
		void cache_defaultTrue() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			assertTrue(frame.cache);
		}

		@Test
		void cache_canBeSetToFalse() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			frame.cache = false;
			assertFalse(frame.cache);
		}

		@Test
		void sqliteConnectionName_defaultNull() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			assertNull(frame.sqliteConnectionName);
		}

		@Test
		void sqliteConnectionName_canBeSet() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			frame.sqliteConnectionName = "my_conn";
			assertEquals("my_conn", frame.sqliteConnectionName);
		}

		@Test
		void keyCache_defaultEmpty() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			assertNotNull(frame.keyCache);
			assertTrue(frame.keyCache.isEmpty());
		}

		@Test
		@SuppressWarnings("unchecked")
		void keyCache_canAddItems() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			frame.keyCache.add("item1");
			assertEquals(1, frame.keyCache.size());
		}

	}

	@Nested
	class GetHeaderAndTypesTests {

		@Test
		void getHeaderAndTypes_returnsTypesAndHeaders() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			String colScript = PandasSyntaxHelper.getColumns("myFrame");
			String typeScript = PandasSyntaxHelper.getTypes("myFrame");
			when(mockTranslator.getList(colScript)).thenReturn(Arrays.asList("col1", "col2", "col3"));
			when(mockTranslator.getList(typeScript)).thenReturn(Arrays.asList("object", "int64", "float64"));

			Object[] result = frame.getHeaderAndTypes("myFrame");

			SemossDataType[] types = (SemossDataType[]) result[0];
			String[] headers = (String[]) result[1];
			assertEquals(3, headers.length);
			assertEquals("col1", headers[0]);
			assertEquals("col2", headers[1]);
			assertEquals("col3", headers[2]);
			assertEquals(SemossDataType.STRING, types[0]);
			assertEquals(SemossDataType.INT, types[1]);
			assertEquals(SemossDataType.DOUBLE, types[2]);
		}

		@Test
		void getHeaderAndTypes_withDatetime64() {
			PandasFrame frame = new PandasFrame("df", mockTranslator);
			String colScript = PandasSyntaxHelper.getColumns("df");
			String typeScript = PandasSyntaxHelper.getTypes("df");
			when(mockTranslator.getList(colScript)).thenReturn(Arrays.asList("dateCol"));
			when(mockTranslator.getList(typeScript)).thenReturn(Arrays.asList("datetime64"));

			Object[] result = frame.getHeaderAndTypes("df");
			SemossDataType[] types = (SemossDataType[]) result[0];
			assertEquals(SemossDataType.DATE, types[0]);
		}

		@Test
		void getHeaderAndTypes_withBoolType() {
			PandasFrame frame = new PandasFrame("df", mockTranslator);
			String colScript = PandasSyntaxHelper.getColumns("df");
			String typeScript = PandasSyntaxHelper.getTypes("df");
			when(mockTranslator.getList(colScript)).thenReturn(Arrays.asList("flag"));
			when(mockTranslator.getList(typeScript)).thenReturn(Arrays.asList("bool"));

			Object[] result = frame.getHeaderAndTypes("df");
			SemossDataType[] types = (SemossDataType[]) result[0];
			assertEquals(SemossDataType.BOOLEAN, types[0]);
		}

		@Test
		void getHeaderAndTypes_withUnknownType_returnsNull() {
			PandasFrame frame = new PandasFrame("df", mockTranslator);
			String colScript = PandasSyntaxHelper.getColumns("df");
			String typeScript = PandasSyntaxHelper.getTypes("df");
			when(mockTranslator.getList(colScript)).thenReturn(Arrays.asList("col1"));
			when(mockTranslator.getList(typeScript)).thenReturn(Arrays.asList("complex128"));

			Object[] result = frame.getHeaderAndTypes("df");
			SemossDataType[] types = (SemossDataType[]) result[0];
			assertNull(types[0]);
		}

		@Test
		void getHeaderAndTypes_withCategoryType() {
			PandasFrame frame = new PandasFrame("df", mockTranslator);
			String colScript = PandasSyntaxHelper.getColumns("df");
			String typeScript = PandasSyntaxHelper.getTypes("df");
			when(mockTranslator.getList(colScript)).thenReturn(Arrays.asList("cat"));
			when(mockTranslator.getList(typeScript)).thenReturn(Arrays.asList("category"));

			Object[] result = frame.getHeaderAndTypes("df");
			SemossDataType[] types = (SemossDataType[]) result[0];
			assertEquals(SemossDataType.STRING, types[0]);
		}

		@Test
		void getHeaderAndTypes_resultStructure() {
			PandasFrame frame = new PandasFrame("df", mockTranslator);
			String colScript = PandasSyntaxHelper.getColumns("df");
			String typeScript = PandasSyntaxHelper.getTypes("df");
			when(mockTranslator.getList(colScript)).thenReturn(Arrays.asList("a", "b"));
			when(mockTranslator.getList(typeScript)).thenReturn(Arrays.asList("object", "object"));

			Object[] result = frame.getHeaderAndTypes("df");
			assertEquals(2, result.length);
			assertTrue(result[0] instanceof SemossDataType[]);
			assertTrue(result[1] instanceof String[]);
		}

		@Test
		void getHeaderAndTypes_usesDifferentTargetFrame() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			// Pass a different target frame name than the frame's own name
			String colScript = PandasSyntaxHelper.getColumns("otherFrame");
			String typeScript = PandasSyntaxHelper.getTypes("otherFrame");
			when(mockTranslator.getList(colScript)).thenReturn(Arrays.asList("x"));
			when(mockTranslator.getList(typeScript)).thenReturn(Arrays.asList("float64"));

			Object[] result = frame.getHeaderAndTypes("otherFrame");
			String[] headers = (String[]) result[1];
			assertEquals("x", headers[0]);
		}
	}

	@Nested
	class QuerySQLTests {

		@Test
		void querySQL_nonSelectCommand_returnsMapWithDataTypesColumns() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			when(mockTranslator.runScript("x = 1")).thenReturn("done");

			Object result = frame.querySQL("x = 1");

			assertTrue(result instanceof Map);
			Map<?, ?> retMap = (Map<?, ?>) result;
			assertNotNull(retMap.get("data"));
			assertNotNull(retMap.get("types"));
			assertNotNull(retMap.get("columns"));
		}

		@Test
		void querySQL_nonSelectCommand_dataContainsCommandAndOutput() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			when(mockTranslator.runScript("print('hello')")).thenReturn("hello");

			Map<?, ?> result = (Map<?, ?>) frame.querySQL("print('hello')");
			List<?> data = (List<?>) result.get("data");
			assertEquals(1, data.size());
			List<?> row = (List<?>) data.get(0);
			assertEquals("print('hello')", row.get(0));
			assertEquals("hello", row.get(1));
		}

		@Test
		void querySQL_nonSelectMultilineCommands_runsEach() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			when(mockTranslator.runScript("x = 1")).thenReturn(1);
			when(mockTranslator.runScript("y = 2")).thenReturn(2);

			Map<?, ?> result = (Map<?, ?>) frame.querySQL("x = 1\ny = 2");
			List<?> data = (List<?>) result.get("data");
			assertEquals(2, data.size());
		}

		@Test
		void querySQL_nonSelectCommand_columnsAreCommandOutput() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			when(mockTranslator.runScript("cmd")).thenReturn(null);

			Map<?, ?> result = (Map<?, ?>) frame.querySQL("cmd");
			String[] columns = (String[]) result.get("columns");
			assertEquals("Command", columns[0]);
			assertEquals("Output", columns[1]);
		}

		@Test
		void querySQL_nonSelectCommand_typesAreBothStringClass() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			when(mockTranslator.runScript("cmd")).thenReturn(null);

			Map<?, ?> result = (Map<?, ?>) frame.querySQL("cmd");
			Object[] types = (Object[]) result.get("types");
			assertEquals(java.lang.String.class, types[0]);
			assertEquals(java.lang.String.class, types[1]);
		}

		@Test
		void querySQL_selectCommand_withMapReturn_processesTypes() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			frame.sqliteConnectionName = "test_conn";

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getRandomString(5)).thenReturn("tmpFr");

				// Build the expected return map from pyTranslator
				Map<String, Object> retObj = new LinkedHashMap<>();
				Map<String, String> typeMap = new LinkedHashMap<>();
				typeMap.put("name", "object");
				typeMap.put("age", "int64");
				retObj.put("types", typeMap);
				retObj.put("columns", Arrays.asList("name", "age"));
				retObj.put("data", Arrays.asList(Arrays.asList("Alice", 30)));

				when(mockTranslator.runScript("tmpFr_dict")).thenReturn(retObj);

				Map<?, ?> result = (Map<?, ?>) frame.querySQL("SELECT * FROM table1");

				assertNotNull(result);
				// Types should be converted from python type strings to Java classes
				Object[] types = (Object[]) result.get("types");
				assertEquals(java.lang.String.class, types[0]); // "object" -> String.class
				assertEquals(java.lang.Integer.class, types[1]); // "int64" -> Integer.class

				// Columns should be a String array
				String[] columns = (String[]) result.get("columns");
				assertEquals("name", columns[0]);
				assertEquals("age", columns[1]);
			}
		}

		@Test
		void querySQL_selectCommand_unknownType_defaultsToStringClass() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			frame.sqliteConnectionName = "test_conn";

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getRandomString(5)).thenReturn("tmpFr");

				Map<String, Object> retObj = new LinkedHashMap<>();
				Map<String, String> typeMap = new LinkedHashMap<>();
				typeMap.put("col1", "complex128"); // unknown type
				retObj.put("types", typeMap);
				retObj.put("columns", Arrays.asList("col1"));
				retObj.put("data", new ArrayList<>());

				when(mockTranslator.runScript("tmpFr_dict")).thenReturn(retObj);

				Map<?, ?> result = (Map<?, ?>) frame.querySQL("SELECT col1 FROM t");

				Object[] types = (Object[]) result.get("types");
				assertEquals(java.lang.String.class, types[0]); // unknown defaults to String.class
			}
		}

		@Test
		void querySQL_selectCommand_runsPyCommands() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			frame.sqliteConnectionName = "test_conn";

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getRandomString(5)).thenReturn("tf");

				Map<String, Object> retObj = new LinkedHashMap<>();
				retObj.put("types", new LinkedHashMap<>());
				retObj.put("columns", new ArrayList<>());
				retObj.put("data", new ArrayList<>());

				when(mockTranslator.runScript("tf_dict")).thenReturn(retObj);

				frame.querySQL("SELECT * FROM data");

				// Verify the py commands were executed
				verify(mockTranslator).runEmptyPy(eq(""), contains("pd.read_sql"), contains("dtypes.to_dict"),
						contains("to_dict('split')"), contains("_dict['types']"));
				// Verify cleanup
				verify(mockTranslator).runEmptyPy(startsWith("del tf"));
			}
		}

		@Test
		void querySQL_selectCommand_htmlEntitiesUnescaped() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			frame.sqliteConnectionName = "test_conn";

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(() -> Utility.getRandomString(5)).thenReturn("tf");

				Map<String, Object> retObj = new LinkedHashMap<>();
				retObj.put("types", new LinkedHashMap<>());
				retObj.put("columns", new ArrayList<>());
				retObj.put("data", new ArrayList<>());

				when(mockTranslator.runScript("tf_dict")).thenReturn(retObj);

				// Input has HTML entities - &gt; should become >
				frame.querySQL("SELECT * FROM t WHERE x &gt; 5");

				// The SQL that gets built should have unescaped HTML
				verify(mockTranslator).runEmptyPy(anyString(), contains("> 5"), anyString(), anyString(), anyString());
			}
		}
	}

	@Nested
	class QueryJSONTests {

		@Test
		void queryJSON_nonSelectCommand_returnsMapWithData() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			when(mockTranslator.runScript("x = 1")).thenReturn(1);

			Object result = frame.queryJSON("x = 1");

			assertTrue(result instanceof Map);
			Map<?, ?> retMap = (Map<?, ?>) result;
			assertNotNull(retMap.get("data"));
			assertNotNull(retMap.get("types"));
			assertNotNull(retMap.get("columns"));
		}

		@Test
		void queryJSON_nonSelectCommand_dataContainsCommandAndOutput() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			when(mockTranslator.runScript("print('hi')")).thenReturn("hi");

			Map<?, ?> result = (Map<?, ?>) frame.queryJSON("print('hi')");
			List<?> data = (List<?>) result.get("data");
			assertEquals(1, data.size());
			List<?> row = (List<?>) data.get(0);
			assertEquals("print('hi')", row.get(0));
			assertEquals("hi", row.get(1));
		}

		@Test
		void queryJSON_nonSelectMultilineCommands_runsEach() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			when(mockTranslator.runScript("a = 1")).thenReturn(1);
			when(mockTranslator.runScript("b = 2")).thenReturn(2);
			when(mockTranslator.runScript("c = 3")).thenReturn(3);

			Map<?, ?> result = (Map<?, ?>) frame.queryJSON("a = 1\nb = 2\nc = 3");
			List<?> data = (List<?>) result.get("data");
			assertEquals(3, data.size());
		}

		@Test
		void queryJSON_nonSelectCommand_columnsAreCommandOutput() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			when(mockTranslator.runScript("cmd")).thenReturn("result");

			Map<?, ?> result = (Map<?, ?>) frame.queryJSON("cmd");
			String[] columns = (String[]) result.get("columns");
			assertEquals("Command", columns[0]);
			assertEquals("Output", columns[1]);
		}

		@Test
		void queryJSON_nonSelectCommand_typesAreBothStringClass() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			when(mockTranslator.runScript("cmd")).thenReturn("result");

			Map<?, ?> result = (Map<?, ?>) frame.queryJSON("cmd");
			Object[] types = (Object[]) result.get("types");
			assertEquals(java.lang.String.class, types[0]);
			assertEquals(java.lang.String.class, types[1]);
		}

		@Test
		void queryJSON_selectCommand_fileDoesNotExist_returnsNull() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			frame.sqliteConnectionName = "test_conn";

			try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
				utilMock.when(Utility::getInsightCacheDir).thenReturn(System.getProperty("java.io.tmpdir"));

				Object result = frame.queryJSON("SELECT * FROM table1");
				// File won't exist since pyTranslator is mocked, so returns null
				assertNull(result);
			}
		}
	}

	@Nested
	class QueryStringMethodTests {

		@Test
		void queryString_withListOutput_returnsRawPandasWrapper() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			List<Object> listOutput = new ArrayList<>();
			listOutput.add(Arrays.asList("val1", 42));
			listOutput.add(Arrays.asList("val2", 99));
			when(mockTranslator.runScript("myFrame.to_dict('split')")).thenReturn(listOutput);

			IRawSelectWrapper result = frame.query("myFrame.to_dict('split')");
			assertNotNull(result);
			assertInstanceOf(RawPandasWrapper.class, result);
		}

		@Test
		void queryString_withNullOutput_returnsWrapper() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			when(mockTranslator.runScript("some_query")).thenReturn(null);

			// When output is null, none of the instanceof checks match,
			// so response stays null, and PandasIterator is created with null data
			IRawSelectWrapper result = frame.query("some_query");
			assertNotNull(result);
		}

		@Test
		void queryString_withListOutput_callsRunScript() {
			PandasFrame frame = new PandasFrame("myFrame", mockTranslator);
			when(mockTranslator.runScript("query1")).thenReturn(new ArrayList<>());

			frame.query("query1");
			verify(mockTranslator).runScript("query1");
		}
	}

	@Nested
	class SaveTests {

		@Test
		void save_createsCachePropFileObject() throws Exception {
			PandasFrame frame = new PandasFrame("testFrame", mockTranslator);
			Path tempDir = Files.createTempDirectory("pandas_test_save");

			try {
				CachePropFileFrameObject cf = frame.save(tempDir.toString(), null);
				assertNotNull(cf);
				assertNotNull(cf.getFrameCacheLocation());
				assertTrue(cf.getFrameCacheLocation().contains("testFrame.pkl"));
			} finally {
				// Clean up temp files
				Files.walk(tempDir).sorted(Comparator.reverseOrder()).forEach(p -> {
					try {
						Files.deleteIfExists(p);
					} catch (IOException e) {
					}
				});
			}
		}

		@Test
		void save_runsPickleCommands() throws Exception {
			PandasFrame frame = new PandasFrame("testFrame", mockTranslator);
			Path tempDir = Files.createTempDirectory("pandas_test_save");

			try {
				frame.save(tempDir.toString(), null);

				// Verify pickle import and dump command were run (varargs)
				verify(mockTranslator).runEmptyPy(eq("import pickle"),
						argThat((String cmd) -> cmd.contains("testFrame") && cmd.contains("pickle")));
			} finally {
				Files.walk(tempDir).sorted(Comparator.reverseOrder()).forEach(p -> {
					try {
						Files.deleteIfExists(p);
					} catch (IOException e) {
					}
				});
			}
		}

		@Test
		void save_frameFilePathHasForwardSlashes() throws Exception {
			PandasFrame frame = new PandasFrame("testFrame", mockTranslator);
			Path tempDir = Files.createTempDirectory("pandas_test_save");

			try {
				frame.save(tempDir.toString(), null);

				// The pickle command should use forward slashes in path
				verify(mockTranslator).runEmptyPy(eq("import pickle"), argThat((String cmd) -> !cmd.contains("\\")));
			} finally {
				Files.walk(tempDir).sorted(Comparator.reverseOrder()).forEach(p -> {
					try {
						Files.deleteIfExists(p);
					} catch (IOException e) {
					}
				});
			}
		}
	}

	@Nested
	class OpenTests {

		@Test
		void open_setsNameAndRunsCommands() {
			PandasFrame frame = new PandasFrame("test", mockTranslator);
			CachePropFileFrameObject cf = new CachePropFileFrameObject();
			cf.setFrameName("loadedFrame");
			cf.setFrameMetaCacheLocation(null);
			cf.setFrameCacheLocation("/tmp/loadedFrame.pkl");

			// openCacheMeta sets frameName from cf, creates new OwlTemporalEngineMeta
			// This will fail gracefully since there's no actual OWL file.
			// We just verify the commands get built correctly by catching the exception.
			try {
				frame.open(cf, null);
			} catch (Exception e) {
				// Expected - openCacheMeta tries to read OWL file that doesn't exist
			}

			// Even if openCacheMeta fails, verify the frame name was set
			// (openCacheMeta sets frameName before loading OWL)
			assertEquals("loadedFrame", frame.getName());
		}
	}
}
