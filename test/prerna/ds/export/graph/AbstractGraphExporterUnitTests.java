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
package prerna.ds.export.graph;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class AbstractGraphExporterUnitTests {

    // Concrete subclass for testing the abstract class
    private static class TestGraphExporter extends AbstractGraphExporter {
        @Override public boolean hasNextEdge() { return false; }
        @Override public Map<String, Object> getNextEdge() { return null; }
        @Override public boolean hasNextVert() { return false; }
        @Override public Map<String, Object> getNextVert() { return null; }
        @Override public Object getData() { return null; }

        // Expose protected method for testing
        public void testAddVertCount(String type) { addVertCount(type); }
    }

    private TestGraphExporter exporter;

    @BeforeEach
    void setUp() {
        exporter = new TestGraphExporter();
    }

    @Nested
    class GetVertCountsTests {
        @Test void initiallyEmpty() {
            Map<String, Integer> counts = exporter.getVertCounts();
            assertNotNull(counts);
            assertTrue(counts.isEmpty());
        }

        @Test void returnsInternalMap() {
            exporter.testAddVertCount("Person");
            Map<String, Integer> counts = exporter.getVertCounts();
            assertEquals(1, counts.get("Person"));
        }
    }

    @Nested
    class AddVertCountTests {
        @Test void addNewType_setsCountTo1() {
            exporter.testAddVertCount("City");
            assertEquals(1, exporter.getVertCounts().get("City"));
        }

        @Test void addSameTypeTwice_incrementsTo2() {
            exporter.testAddVertCount("City");
            exporter.testAddVertCount("City");
            assertEquals(2, exporter.getVertCounts().get("City"));
        }

        @Test void addSameTypeMultipleTimes() {
            for (int i = 0; i < 10; i++) {
                exporter.testAddVertCount("Node");
            }
            assertEquals(10, exporter.getVertCounts().get("Node"));
        }

        @Test void addMultipleTypes_trackedSeparately() {
            exporter.testAddVertCount("Person");
            exporter.testAddVertCount("City");
            exporter.testAddVertCount("Person");
            exporter.testAddVertCount("Person");
            exporter.testAddVertCount("City");

            Map<String, Integer> counts = exporter.getVertCounts();
            assertEquals(2, counts.size());
            assertEquals(3, counts.get("Person"));
            assertEquals(2, counts.get("City"));
        }

        @Test void addManyDistinctTypes() {
            exporter.testAddVertCount("A");
            exporter.testAddVertCount("B");
            exporter.testAddVertCount("C");
            exporter.testAddVertCount("D");

            Map<String, Integer> counts = exporter.getVertCounts();
            assertEquals(4, counts.size());
            assertEquals(1, counts.get("A"));
            assertEquals(1, counts.get("B"));
            assertEquals(1, counts.get("C"));
            assertEquals(1, counts.get("D"));
        }

        @Test void emptyStringType() {
            exporter.testAddVertCount("");
            assertEquals(1, exporter.getVertCounts().get(""));
        }

        @Test void caseSensitive() {
            exporter.testAddVertCount("person");
            exporter.testAddVertCount("Person");
            exporter.testAddVertCount("PERSON");

            Map<String, Integer> counts = exporter.getVertCounts();
            assertEquals(3, counts.size());
            assertEquals(1, counts.get("person"));
            assertEquals(1, counts.get("Person"));
            assertEquals(1, counts.get("PERSON"));
        }
    }
}
