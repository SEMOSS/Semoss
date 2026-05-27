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

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.*;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PyUtilsUnitTests {

    @Test
    void pyCommandSeparator_isSemicolon() {
        assertEquals(";", PyUtils.PY_COMMAND_SEPARATOR);
    }

    @Nested
    class DetermineStringTypeTests {

        @Test
        void null_returnsNone() {
            assertEquals("None", PyUtils.determineStringType(null));
        }

        @Test
        void integer_returnsStringValue() {
            assertEquals("42", PyUtils.determineStringType(42));
        }

        @Test
        void negativeInteger_returnsStringValue() {
            assertEquals("-5", PyUtils.determineStringType(-5));
        }

        @Test
        void zeroInteger_returnsZero() {
            assertEquals("0", PyUtils.determineStringType(0));
        }

        @Test
        void double_returnsStringValue() {
            assertEquals("3.14", PyUtils.determineStringType(3.14));
        }

        @Test
        void long_returnsStringValue() {
            assertEquals("100", PyUtils.determineStringType(100L));
        }

        @Test
        void float_returnsStringValue() {
            String result = PyUtils.determineStringType(1.5f);
            assertEquals("1.5", result);
        }

        @Test
        void booleanTrue_returnsTrue() {
            assertEquals("True", PyUtils.determineStringType(Boolean.TRUE));
        }

        @Test
        void booleanFalse_returnsFalse() {
            assertEquals("False", PyUtils.determineStringType(Boolean.FALSE));
        }

        @Test
        void plainString_returnsEscapedQuotedString() {
            assertEquals("\"hello\"", PyUtils.determineStringType("hello"));
        }

        @Test
        void emptyString_returnsEmptyQuotedString() {
            assertEquals("\"\"", PyUtils.determineStringType(""));
        }

        @Test
        void stringWithBackslash_escapesBackslash() {
            String result = PyUtils.determineStringType("path\\to\\file");
            assertEquals("\"path\\\\to\\\\file\"", result);
        }

        @Test
        void stringWithDoubleQuote_escapesDoubleQuote() {
            String result = PyUtils.determineStringType("say \"hi\"");
            assertEquals("\"say \\\"hi\\\"\"", result);
        }

        @Test
        void stringWithNewline_escapesNewline() {
            String result = PyUtils.determineStringType("line1\nline2");
            assertEquals("\"line1\\nline2\"", result);
        }

        @Test
        void stringWithCarriageReturn_escapesCarriageReturn() {
            String result = PyUtils.determineStringType("line1\rline2");
            assertEquals("\"line1\\rline2\"", result);
        }

        @Test
        void stringWithTab_escapesTab() {
            String result = PyUtils.determineStringType("col1\tcol2");
            assertEquals("\"col1\\tcol2\"", result);
        }

        @Test
        void emptyArrayList_returnsEmptyList() {
            assertEquals("[]", PyUtils.determineStringType(new ArrayList<>()));
        }

        @Test
        void arrayListWithMixedTypes_returnsFormattedList() {
            ArrayList<Object> list = new ArrayList<>();
            list.add(1);
            list.add("hello");
            list.add(true);
            String result = PyUtils.determineStringType(list);
            assertEquals("[1, \"hello\", True]", result);
        }

        @Test
        void objectArray_returnsFormattedList() {
            Object[] arr = new Object[]{1, "test", 3.14};
            String result = PyUtils.determineStringType(arr);
            assertEquals("[1, \"test\", 3.14]", result);
        }

        @Test
        void nestedList_returnsNestedFormattedList() {
            ArrayList<Object> inner = new ArrayList<>();
            inner.add(1);
            inner.add(2);
            ArrayList<Object> outer = new ArrayList<>();
            outer.add(inner);
            outer.add("end");
            String result = PyUtils.determineStringType(outer);
            assertEquals("[[1, 2], \"end\"]", result);
        }

        @Test
        void emptyMap_returnsEmptyDict() {
            Map<String, Object> map = new LinkedHashMap<>();
            assertEquals("{}", PyUtils.determineStringType(map));
        }

        @Test
        void mapWithStringValues_returnsPythonDict() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("name", "Alice");
            map.put("age", 30);
            String result = PyUtils.determineStringType(map);
            assertEquals("{\"name\": \"Alice\", \"age\": 30}", result);
        }

        @Test
        void mapWithNestedValues_returnsPythonDict() {
            Map<String, Object> inner = new LinkedHashMap<>();
            inner.put("x", 1);
            Map<String, Object> outer = new LinkedHashMap<>();
            outer.put("nested", inner);
            String result = PyUtils.determineStringType(outer);
            assertEquals("{\"nested\": {\"x\": 1}}", result);
        }

        @Test
        void emptySet_returnsEmptySet() {
            Set<Object> set = new LinkedHashSet<>();
            assertEquals("{}", PyUtils.determineStringType(set));
        }

        @Test
        void setWithElements_returnsPythonSet() {
            Set<Object> set = new LinkedHashSet<>();
            set.add(1);
            set.add("hello");
            String result = PyUtils.determineStringType(set);
            assertEquals("{1, \"hello\"}", result);
        }

        @Test
        void fileObject_returnsRawStringPath() {
            File f = new File("/home/user/data.csv");
            String result = PyUtils.determineStringType(f);
            // File.getAbsolutePath may vary by OS, but should contain r" prefix
            assertTrue(result.startsWith("r\""));
            assertTrue(result.endsWith("\""));
            assertTrue(result.contains("data.csv"));
            // Should not contain backslashes (replaced with forward slashes)
            assertFalse(result.contains("\\"));
        }

        @Test
        void singleElementList_returnsFormattedList() {
            List<Object> list = List.of("only");
            String result = PyUtils.determineStringType(list);
            assertEquals("[\"only\"]", result);
        }

        @Test
        void listWithNulls_handlesNullElements() {
            ArrayList<Object> list = new ArrayList<>();
            list.add(null);
            String result = PyUtils.determineStringType(list);
            assertEquals("[None]", result);
        }

        @Test
        void mapWithNullValue_handlesNullValue() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("key", null);
            String result = PyUtils.determineStringType(map);
            assertEquals("{\"key\": None}", result);
        }
    }

    @Nested
    class AppendVenvExecutableTests {

        @Test
        void appendVenvPythonExecutable_containsPython() {
            String result = PyUtils.appendVenvPythonExecutable("/path/to/venv/");
            assertNotNull(result);
            assertTrue(result.contains("python"));
        }

        @Test
        void appendVenvPipExecutable_containsPip() {
            String result = PyUtils.appendVenvPipExecutable("/path/to/venv");
            assertNotNull(result);
            assertTrue(result.contains("pip"));
        }
    }

    @Nested
    class AdditionalCoverageTests {

        @Test
        void determineStringType_insightObject() {
            prerna.om.Insight insight = org.mockito.Mockito.mock(prerna.om.Insight.class);
            org.mockito.Mockito.when(insight.getInsightId()).thenReturn("test-id-123");
            String result = PyUtils.determineStringType(insight);
            assertEquals("\"test-id-123\"", result);
        }

        @Test
        void determineStringType_insightWithSpecialChars() {
            prerna.om.Insight insight = org.mockito.Mockito.mock(prerna.om.Insight.class);
            org.mockito.Mockito.when(insight.getInsightId()).thenReturn("id\"with\\special");
            String result = PyUtils.determineStringType(insight);
            assertEquals("\"id\\\"with\\\\special\"", result);
        }

        @Test
        void determineStringType_mapWithNullKey() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put(null, "value");
            String result = PyUtils.determineStringType(map);
            assertEquals("{\"\": \"value\"}", result);
        }

        @Test
        void determineStringType_stringWithMultipleEscapes() {
            String result = PyUtils.determineStringType("a\\b\"c\nd\re\tf");
            assertEquals("\"a\\\\b\\\"c\\nd\\re\\tf\"", result);
        }
    }
}
