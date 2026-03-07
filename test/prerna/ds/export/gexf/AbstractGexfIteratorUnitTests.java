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
package prerna.ds.export.gexf;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class AbstractGexfIteratorUnitTests {

    // Concrete subclass for testing abstract class
    private static class TestGexfIterator extends AbstractGexfIterator {
        public TestGexfIterator(String nodeMap, String edgeMap, Map<String, String> aliasMap) {
            super(nodeMap, edgeMap, aliasMap);
        }

        @Override public boolean hasNextNode() { return false; }
        @Override public String getNextNodeString() { return null; }
        @Override public boolean hasNextEdge() { return false; }
        @Override public String getNextEdgeString() { return null; }

        // Expose protected fields for assertions
        public String getNodeMap() { return nodeMap; }
        public String getEdgeMap() { return edgeMap; }
        public String[] getNodeMapSplit() { return nodeMapSplit; }
        public String[] getEdgeMapSplit() { return edgeMapSplit; }
        public Map<String, String> getAliasMap() { return aliasMap; }
        public int getNodeIndex() { return nodeIndex; }
        public int getEdgeIndex() { return edgeIndex; }
    }

    @Nested
    class ConstructorTests {

        @Test void normalValues() {
            Map<String, String> alias = new HashMap<>();
            alias.put("col1", "Column 1");
            TestGexfIterator it = new TestGexfIterator("A;B;C", "X;Y", alias);

            assertEquals("A;B;C", it.getNodeMap());
            assertEquals("X;Y", it.getEdgeMap());
            assertArrayEquals(new String[]{"A", "B", "C"}, it.getNodeMapSplit());
            assertArrayEquals(new String[]{"X", "Y"}, it.getEdgeMapSplit());
            assertEquals("Column 1", it.getAliasMap().get("col1"));
            assertEquals(0, it.getNodeIndex());
            assertEquals(0, it.getEdgeIndex());
        }

        @Test void nullNodeMap_defaultsToEmpty() {
            TestGexfIterator it = new TestGexfIterator(null, "E1;E2", null);
            assertEquals("", it.getNodeMap());
            assertArrayEquals(new String[]{""}, it.getNodeMapSplit());
        }

        @Test void nullEdgeMap_defaultsToEmpty() {
            TestGexfIterator it = new TestGexfIterator("N1", null, null);
            assertEquals("", it.getEdgeMap());
            assertArrayEquals(new String[]{""}, it.getEdgeMapSplit());
        }

        @Test void nullAliasMap_defaultsToEmptyMap() {
            TestGexfIterator it = new TestGexfIterator("N1", "E1", null);
            assertNotNull(it.getAliasMap());
            assertTrue(it.getAliasMap().isEmpty());
        }

        @Test void allNulls() {
            TestGexfIterator it = new TestGexfIterator(null, null, null);
            assertEquals("", it.getNodeMap());
            assertEquals("", it.getEdgeMap());
            assertNotNull(it.getAliasMap());
            assertTrue(it.getAliasMap().isEmpty());
        }

        @Test void emptyStrings() {
            TestGexfIterator it = new TestGexfIterator("", "", new HashMap<>());
            assertEquals("", it.getNodeMap());
            assertEquals("", it.getEdgeMap());
            assertArrayEquals(new String[]{""}, it.getNodeMapSplit());
            assertArrayEquals(new String[]{""}, it.getEdgeMapSplit());
        }

        @Test void singleNodeNoSemicolon() {
            TestGexfIterator it = new TestGexfIterator("OnlyNode", "OnlyEdge", null);
            assertArrayEquals(new String[]{"OnlyNode"}, it.getNodeMapSplit());
            assertArrayEquals(new String[]{"OnlyEdge"}, it.getEdgeMapSplit());
        }

        @Test void multipleSemicolons_splitCorrectly() {
            TestGexfIterator it = new TestGexfIterator("A;B;C;D;E", "X;Y;Z", null);
            assertEquals(5, it.getNodeMapSplit().length);
            assertEquals(3, it.getEdgeMapSplit().length);
        }

        @Test void trailingSemicolon() {
            TestGexfIterator it = new TestGexfIterator("A;B;", "X;", null);
            // Java split with trailing delimiter: "A;B;" -> ["A","B"]
            assertEquals(2, it.getNodeMapSplit().length);
            assertEquals(1, it.getEdgeMapSplit().length);
        }
    }

    @Nested
    class XmlStringTests {

        private TestGexfIterator createDefault() {
            return new TestGexfIterator("N", "E", null);
        }

        @Test void getStartString_containsXmlDeclaration() {
            String start = createDefault().getStartString();
            assertTrue(start.startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
        }

        @Test void getStartString_containsGexfNamespace() {
            String start = createDefault().getStartString();
            assertTrue(start.contains("xmlns=\"http://www.gexf.net/1.2draft\""));
        }

        @Test void getStartString_containsVizNamespace() {
            String start = createDefault().getStartString();
            assertTrue(start.contains("xmlns:viz=\"http://www.gexf.net/1.1draft/viz\""));
        }

        @Test void getStartString_containsGraphTag() {
            String start = createDefault().getStartString();
            assertTrue(start.contains("<graph mode=\"static\" defaultedgetype=\"directed\">"));
        }

        @Test void getStartString_containsVersion() {
            String start = createDefault().getStartString();
            assertTrue(start.contains("version=\"1.2\""));
        }

        @Test void getEndString() {
            assertEquals("</graph></gexf>", createDefault().getEndString());
        }

        @Test void getNodeStart() {
            assertEquals("<nodes>", createDefault().getNodeStart());
        }

        @Test void getNodeEnd() {
            assertEquals("</nodes>", createDefault().getNodeEnd());
        }

        @Test void getEdgeStart() {
            assertEquals("<edges>", createDefault().getEdgeStart());
        }

        @Test void getEdgeEnd() {
            assertEquals("</edges>", createDefault().getEdgeEnd());
        }

        @Test void wellFormedXmlStructure() {
            TestGexfIterator it = createDefault();
            String doc = it.getStartString()
                    + it.getNodeStart() + it.getNodeEnd()
                    + it.getEdgeStart() + it.getEdgeEnd()
                    + it.getEndString();
            assertTrue(doc.contains("<gexf"));
            assertTrue(doc.contains("</gexf>"));
            assertTrue(doc.contains("<graph"));
            assertTrue(doc.contains("</graph>"));
            assertTrue(doc.contains("<nodes></nodes>"));
            assertTrue(doc.contains("<edges></edges>"));
        }
    }
}
