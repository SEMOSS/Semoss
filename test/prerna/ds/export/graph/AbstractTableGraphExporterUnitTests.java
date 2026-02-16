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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class AbstractTableGraphExporterUnitTests {

    // Concrete subclass for testing the abstract class
    private static class TestTableGraphExporter extends AbstractTableGraphExporter {
        @Override public boolean hasNextEdge() { return false; }
        @Override public Map<String, Object> getNextEdge() { return null; }
        @Override public boolean hasNextVert() { return false; }
        @Override public Map<String, Object> getNextVert() { return null; }
        @Override public Object getData() { return null; }

        // Expose protected fields for testing
        public void callParseEdgeHash(Map<String, Set<String>> edgeHash) { parseEdgeHash(edgeHash); }
        public Set<String> getVertices() { return vertices; }
        public Set<String[]> getRelationships() { return relationships; }
        public Iterator<String> getVerticesIterator() { return verticesIterator; }
        public Iterator<String[]> getRelationshipIterator() { return relationshipIterator; }
    }

    private TestTableGraphExporter exporter;

    @BeforeEach
    void setUp() {
        exporter = new TestTableGraphExporter();
    }

    @Nested
    class ParseEdgeHashTests {

        @Test void singleEdge() {
            Map<String, Set<String>> edgeHash = new HashMap<>();
            edgeHash.put("A", Set.of("B"));

            exporter.callParseEdgeHash(edgeHash);

            assertEquals(2, exporter.getVertices().size());
            assertTrue(exporter.getVertices().contains("A"));
            assertTrue(exporter.getVertices().contains("B"));
            assertEquals(1, exporter.getRelationships().size());

            String[] rel = exporter.getRelationships().iterator().next();
            assertEquals("A", rel[0]);
            assertEquals("B", rel[1]);
        }

        @Test void multipleEdgesFromSameSource() {
            Map<String, Set<String>> edgeHash = new HashMap<>();
            Set<String> targets = new HashSet<>();
            targets.add("B");
            targets.add("C");
            targets.add("D");
            edgeHash.put("A", targets);

            exporter.callParseEdgeHash(edgeHash);

            assertEquals(4, exporter.getVertices().size());
            assertTrue(exporter.getVertices().containsAll(Set.of("A", "B", "C", "D")));
            assertEquals(3, exporter.getRelationships().size());

            Set<String> relStrings = exporter.getRelationships().stream()
                    .map(r -> r[0] + "->" + r[1])
                    .collect(Collectors.toSet());
            assertTrue(relStrings.contains("A->B"));
            assertTrue(relStrings.contains("A->C"));
            assertTrue(relStrings.contains("A->D"));
        }

        @Test void multipleSources() {
            Map<String, Set<String>> edgeHash = new HashMap<>();
            edgeHash.put("A", Set.of("B"));
            edgeHash.put("C", Set.of("D"));

            exporter.callParseEdgeHash(edgeHash);

            assertEquals(4, exporter.getVertices().size());
            assertTrue(exporter.getVertices().containsAll(Set.of("A", "B", "C", "D")));
            assertEquals(2, exporter.getRelationships().size());
        }

        @Test void sharedVertices_deduplicatedInSet() {
            // A->B and B->C means B is both a target and source
            Map<String, Set<String>> edgeHash = new LinkedHashMap<>();
            edgeHash.put("A", Set.of("B"));
            edgeHash.put("B", Set.of("C"));

            exporter.callParseEdgeHash(edgeHash);

            // B appears as both start node and end node but should be in the set once
            assertEquals(3, exporter.getVertices().size());
            assertTrue(exporter.getVertices().containsAll(Set.of("A", "B", "C")));
            assertEquals(2, exporter.getRelationships().size());
        }

        @Test void selfLoop() {
            Map<String, Set<String>> edgeHash = new HashMap<>();
            edgeHash.put("A", Set.of("A"));

            exporter.callParseEdgeHash(edgeHash);

            assertEquals(1, exporter.getVertices().size());
            assertTrue(exporter.getVertices().contains("A"));
            assertEquals(1, exporter.getRelationships().size());

            String[] rel = exporter.getRelationships().iterator().next();
            assertEquals("A", rel[0]);
            assertEquals("A", rel[1]);
        }

        @Test void emptyEdgeHash() {
            Map<String, Set<String>> edgeHash = new HashMap<>();

            exporter.callParseEdgeHash(edgeHash);

            assertNotNull(exporter.getVertices());
            assertTrue(exporter.getVertices().isEmpty());
            assertNotNull(exporter.getRelationships());
            assertTrue(exporter.getRelationships().isEmpty());
        }

        @Test void sourceWithEmptyTargets() {
            Map<String, Set<String>> edgeHash = new HashMap<>();
            edgeHash.put("A", new HashSet<>());

            exporter.callParseEdgeHash(edgeHash);

            // A is added as a vertex (start node), but no edges
            assertEquals(1, exporter.getVertices().size());
            assertTrue(exporter.getVertices().contains("A"));
            assertTrue(exporter.getRelationships().isEmpty());
        }

        @Test void iteratorsInitialized() {
            Map<String, Set<String>> edgeHash = new HashMap<>();
            edgeHash.put("X", Set.of("Y"));

            exporter.callParseEdgeHash(edgeHash);

            assertNotNull(exporter.getVerticesIterator());
            assertNotNull(exporter.getRelationshipIterator());

            // Should be able to iterate through
            int vertCount = 0;
            while (exporter.getVerticesIterator().hasNext()) {
                exporter.getVerticesIterator().next();
                vertCount++;
            }
            assertEquals(2, vertCount);

            int relCount = 0;
            while (exporter.getRelationshipIterator().hasNext()) {
                exporter.getRelationshipIterator().next();
                relCount++;
            }
            assertEquals(1, relCount);
        }

        @Test void complexGraph() {
            // A->B, A->C, B->C, C->D
            Map<String, Set<String>> edgeHash = new LinkedHashMap<>();
            Set<String> aTargets = new HashSet<>();
            aTargets.add("B");
            aTargets.add("C");
            edgeHash.put("A", aTargets);
            edgeHash.put("B", Set.of("C"));
            edgeHash.put("C", Set.of("D"));

            exporter.callParseEdgeHash(edgeHash);

            assertEquals(4, exporter.getVertices().size());
            assertTrue(exporter.getVertices().containsAll(Set.of("A", "B", "C", "D")));
            assertEquals(4, exporter.getRelationships().size());
        }

        @Test void relationshipArrayStructure() {
            Map<String, Set<String>> edgeHash = new HashMap<>();
            edgeHash.put("Source", Set.of("Target"));

            exporter.callParseEdgeHash(edgeHash);

            String[] rel = exporter.getRelationships().iterator().next();
            assertEquals(2, rel.length);
            assertEquals("Source", rel[0]);
            assertEquals("Target", rel[1]);
        }

        @Test void callingTwice_replacesOldData() {
            Map<String, Set<String>> edgeHash1 = new HashMap<>();
            edgeHash1.put("A", Set.of("B"));
            exporter.callParseEdgeHash(edgeHash1);
            assertEquals(2, exporter.getVertices().size());

            Map<String, Set<String>> edgeHash2 = new HashMap<>();
            edgeHash2.put("X", Set.of("Y", "Z"));
            exporter.callParseEdgeHash(edgeHash2);
            assertEquals(3, exporter.getVertices().size());
            assertTrue(exporter.getVertices().containsAll(Set.of("X", "Y", "Z")));
            assertFalse(exporter.getVertices().contains("A"));
        }
    }

    @Nested
    class InheritsAbstractGraphExporterTests {
        @Test void canUseAddVertCount() {
            // Verify inheritance from AbstractGraphExporter works
            exporter.getVertCounts(); // should not throw
            assertNotNull(exporter.getVertCounts());
        }
    }
}
