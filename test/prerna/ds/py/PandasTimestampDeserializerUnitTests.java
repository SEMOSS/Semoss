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

import org.junit.jupiter.api.Test;

class PandasTimestampDeserializerUnitTests {

    @Test
    void testRemoveTimestampSingleTimestampInArray() {
        String input = "[Timestamp('2023-01-01'), 'other']";
        String result = PandasTimestampDeserializer.removeTimestamp(input);
        assertEquals("['2023-01-01', 'other']", result);
    }

    @Test
    void testRemoveTimestampMultipleTimestamps() {
        String input = "[Timestamp('2023-01-01'), Timestamp('2023-06-15'), 'end']";
        String result = PandasTimestampDeserializer.removeTimestamp(input);
        assertEquals("['2023-01-01', '2023-06-15', 'end']", result);
    }

    @Test
    void testRemoveTimestampInDict() {
        String input = "{Timestamp('2023-01-01'), 'value'}";
        String result = PandasTimestampDeserializer.removeTimestamp(input);
        assertEquals("{'2023-01-01', 'value'}", result);
    }

    @Test
    void testRemoveTimestampNoTimestamps() {
        String input = "['no', 'timestamps', 'here']";
        String result = PandasTimestampDeserializer.removeTimestamp(input);
        assertEquals("['no', 'timestamps', 'here']", result);
    }

    @Test
    void testRemoveTimestampEmptyString() {
        String result = PandasTimestampDeserializer.removeTimestamp("");
        assertEquals("", result);
    }

    @Test
    void testRemoveTimestampFollowedByCloseBracket() {
        String input = "[Timestamp('2023-12-31')]";
        String result = PandasTimestampDeserializer.removeTimestamp(input);
        assertEquals("['2023-12-31']", result);
    }

    @Test
    void testRemoveTimestampFollowedByCloseBrace() {
        String input = "{Timestamp('2023-12-31')}";
        String result = PandasTimestampDeserializer.removeTimestamp(input);
        assertEquals("{'2023-12-31'}", result);
    }

    @Test
    void testRemoveTimestampAfterCommaSpace() {
        String input = "['first', Timestamp('2023-03-15'), 'last']";
        String result = PandasTimestampDeserializer.removeTimestamp(input);
        assertEquals("['first', '2023-03-15', 'last']", result);
    }

    @Test
    void testMapperIsNotNull() {
        assertNotNull(PandasTimestampDeserializer.MAPPER);
    }

    @Test
    void testConstructorCreatesModuleWithoutError() {
        assertDoesNotThrow(() -> new PandasTimestampDeserializer());
    }

    @Test
    void testRemoveTimestampPreservesOtherContent() {
        String input = "[Timestamp('2023-01-01'), 'hello world', 42, true]";
        String result = PandasTimestampDeserializer.removeTimestamp(input);
        // Only the Timestamp wrapper should be removed; other content stays
        assertTrue(result.contains("'hello world'"));
        assertTrue(result.contains("42"));
        assertTrue(result.contains("true"));
        assertTrue(result.contains("'2023-01-01'"));
        assertFalse(result.contains("Timestamp"));
    }

    @Test
    void testRemoveTimestampWithTimePortion() {
        String input = "[Timestamp('2023-01-15 14:30:00'), 'data']";
        String result = PandasTimestampDeserializer.removeTimestamp(input);
        assertEquals("['2023-01-15 14:30:00', 'data']", result);
    }
}
