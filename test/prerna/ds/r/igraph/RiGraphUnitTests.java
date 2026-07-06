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
package prerna.ds.r.igraph;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;

class RiGraphUnitTests {

    private AbstractRJavaTranslator mockTranslator;
    private RiGraph graph;

    @BeforeEach
    void setUp() {
        mockTranslator = mock(AbstractRJavaTranslator.class);
        graph = new RiGraph("testGraph", mockTranslator);
    }

    @Nested
    class ConstantsTests {
        @Test
        void dataMakerName_isRiGraph() {
            assertEquals("RiGraph", RiGraph.DATA_MAKER_NAME);
        }
    }

    @Nested
    class ConstructorTests {
        @Test
        void constructorWithVarName_setsGraphName() {
            RiGraph g = new RiGraph("myGraph");
            assertNotNull(g);
        }

        @Test
        void constructorWithTranslator_callsStartR() {
            verify(mockTranslator).startR();
        }

        @Test
        void constructorWithTranslator_loadsIgraphLibrary() {
            verify(mockTranslator).executeEmptyR("library(igraph)");
        }

        @Test
        void constructorWithTranslator_createsEmptyGraph() {
            verify(mockTranslator).executeEmptyR("testGraph<- make_empty_graph()");
        }
    }

    @Nested
    class SetRJavaTranslatorTests {
        @Test
        void setRJavaTranslator_callsStartR() {
            AbstractRJavaTranslator newTranslator = mock(AbstractRJavaTranslator.class);
            RiGraph g = new RiGraph("g2");
            g.setRJavaTranslator(newTranslator);

            verify(newTranslator).startR();
        }

        @Test
        void setRJavaTranslator_loadsIgraph() {
            AbstractRJavaTranslator newTranslator = mock(AbstractRJavaTranslator.class);
            RiGraph g = new RiGraph("g2");
            g.setRJavaTranslator(newTranslator);

            verify(newTranslator).executeEmptyR("library(igraph)");
        }

        @Test
        void setRJavaTranslator_createsEmptyGraphWithCorrectName() {
            AbstractRJavaTranslator newTranslator = mock(AbstractRJavaTranslator.class);
            RiGraph g = new RiGraph("myVar");
            g.setRJavaTranslator(newTranslator);

            verify(newTranslator).executeEmptyR("myVar<- make_empty_graph()");
        }
    }

    @Nested
    class IsEmptyTests {
        @Test
        void isEmpty_true_whenSizeIsZero() {
            when(mockTranslator.getInt("length(as_ids(testGraph))")).thenReturn(0);
            assertTrue(graph.isEmpty());
        }

        @Test
        void isEmpty_false_whenSizeIsNonZero() {
            when(mockTranslator.getInt("length(as_ids(testGraph))")).thenReturn(5);
            assertFalse(graph.isEmpty());
        }

        @Test
        void isEmpty_false_whenSizeIsOne() {
            when(mockTranslator.getInt("length(as_ids(testGraph))")).thenReturn(1);
            assertFalse(graph.isEmpty());
        }
    }

    @Nested
    class GetFrameTypeTests {
        @Test
        void getFrameType_returnsIGRAPH() {
            assertEquals(DataFrameTypeEnum.IGRAPH, graph.getFrameType());
        }
    }

    @Nested
    class GetDataMakerNameTests {
        @Test
        void getDataMakerName_returnsRiGraph() {
            assertEquals("RiGraph", graph.getDataMakerName());
        }
    }

    @Nested
    class SizeTests {
        @Test
        void size_alwaysReturnsZero() {
            assertEquals(0, graph.size("anything"));
        }

        @Test
        void size_returnsZero_forAnyInput() {
            assertEquals(0, graph.size("testGraph"));
        }
    }

    @Nested
    class StubMethodTests {
        @Test
        void getMax_returnsNull() {
            assertNull(graph.getMax("anyCol"));
        }

        @Test
        void getMin_returnsNull() {
            assertNull(graph.getMin("anyCol"));
        }

        @Test
        void queryString_returnsNull() {
            assertNull(graph.query("SELECT * FROM something"));
        }

        @Test
        void querySelectQueryStruct_returnsNull() {
            assertNull(graph.query(mock(prerna.query.querystruct.SelectQueryStruct.class)));
        }

        @Test
        void getQueryInterpreter_returnsNull() {
            assertNull(graph.getQueryInterpreter());
        }

        @Test
        void save_returnsNull() {
            assertNull(graph.save("folder", null));
        }

        @Test
        void open_doesNotThrow() {
            assertDoesNotThrow(() -> graph.open(null, null));
        }

        @Test
        void close_doesNotThrow() {
            assertDoesNotThrow(() -> graph.close());
        }

        @Test
        void scaledUniqueIterator_returnsNull() {
            assertNull(graph.scaledUniqueIterator("col", List.of("attr")));
        }

        @Test
        void addRow_doesNotThrow() {
            assertDoesNotThrow(() -> graph.addRow(new Object[]{"a"}, new String[]{"col"}));
        }

        @Test
        void processDataMakerComponent_doesNotThrow() {
            assertDoesNotThrow(() -> graph.processDataMakerComponent(null));
        }
    }

    @Nested
    class RemoveColumnTests {
        @Test
        void removeColumn_callsDeleteVerticesWithCorrectScript() {
            graph.removeColumn("Title");

            String expected = "testGraph <- delete_vertices(testGraph, V(testGraph)"
                    + "[vertex_attr(testGraph, \"type\") == \"Title\"])";
            verify(mockTranslator).executeEmptyR(expected);
        }

        @Test
        void removeColumn_differentColumn() {
            graph.removeColumn("Genre");

            String expected = "testGraph <- delete_vertices(testGraph, V(testGraph)"
                    + "[vertex_attr(testGraph, \"type\") == \"Genre\"])";
            verify(mockTranslator).executeEmptyR(expected);
        }
    }

    @Nested
    class AddRelationshipArrayTests {

        @Test
        void addRelationship_withRelationship_executesVertexAndEdgeScripts() {
            String[] headers = {"Title", "Genre"};
            Object[] values = {"Inception", "SciFi"};
            Map<Integer, Set<Integer>> cardinality = new HashMap<>();
            Set<Integer> ends = new HashSet<>();
            ends.add(1);
            cardinality.put(0, ends);

            graph.addRelationship(headers, values, cardinality);

            // Should execute a script containing vertex and edge upserts
            ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
            // Constructor calls: startR, library(igraph), make_empty_graph
            // addRelationship call: the combined script
            verify(mockTranslator, atLeast(3)).executeEmptyR(captor.capture());
            List<String> allCalls = captor.getAllValues();
            String lastCall = allCalls.get(allCalls.size() - 1);

            // Verify the script contains vertex upserts for both nodes
            assertTrue(lastCall.contains("Title:Inception"));
            assertTrue(lastCall.contains("Genre:SciFi"));
            // Verify it contains an edge upsert
            assertTrue(lastCall.contains("add_edges"));
        }

        @Test
        void addRelationship_noRelationship_insertsSingleNode() {
            String[] headers = {"Title"};
            Object[] values = {"Inception"};
            Map<Integer, Set<Integer>> cardinality = new HashMap<>();
            // Empty cardinality - no relationships

            graph.addRelationship(headers, values, cardinality);

            ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
            verify(mockTranslator, atLeast(3)).executeEmptyR(captor.capture());
            List<String> allCalls = captor.getAllValues();
            String lastCall = allCalls.get(allCalls.size() - 1);

            // Single node insertion
            assertTrue(lastCall.contains("Title:Inception"));
            assertTrue(lastCall.contains("add_vertices"));
            assertFalse(lastCall.contains("add_edges"));
        }

        @Test
        void addRelationship_nullEndIndices_skipsEntry() {
            String[] headers = {"Title", "Genre"};
            Object[] values = {"Inception", "SciFi"};
            Map<Integer, Set<Integer>> cardinality = new HashMap<>();
            cardinality.put(0, null); // null end indices

            graph.addRelationship(headers, values, cardinality);

            // With null endIndices, hasRel stays false -> inserts single node
            ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
            verify(mockTranslator, atLeast(3)).executeEmptyR(captor.capture());
            List<String> allCalls = captor.getAllValues();
            String lastCall = allCalls.get(allCalls.size() - 1);

            assertTrue(lastCall.contains("Title:Inception"));
            assertFalse(lastCall.contains("add_edges"));
        }

        @Test
        void addRelationship_multipleEndNodes_createsMultipleEdges() {
            String[] headers = {"Title", "Genre", "Director"};
            Object[] values = {"Inception", "SciFi", "Nolan"};
            Map<Integer, Set<Integer>> cardinality = new HashMap<>();
            Set<Integer> ends = new HashSet<>();
            ends.add(1);
            ends.add(2);
            cardinality.put(0, ends);

            graph.addRelationship(headers, values, cardinality);

            ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
            verify(mockTranslator, atLeast(3)).executeEmptyR(captor.capture());
            List<String> allCalls = captor.getAllValues();
            String lastCall = allCalls.get(allCalls.size() - 1);

            // Should contain vertex upserts for all three nodes
            assertTrue(lastCall.contains("Title:Inception"));
            assertTrue(lastCall.contains("Genre:SciFi"));
            assertTrue(lastCall.contains("Director:Nolan"));
        }
    }

    @Nested
    class UpsertVertexSyntaxTests {
        @Test
        void addRelationship_vertexScript_containsConditionalCheck() {
            String[] headers = {"Title", "Genre"};
            Object[] values = {"Inception", "SciFi"};
            Map<Integer, Set<Integer>> cardinality = new HashMap<>();
            Set<Integer> ends = new HashSet<>();
            ends.add(1);
            cardinality.put(0, ends);

            graph.addRelationship(headers, values, cardinality);

            ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
            verify(mockTranslator, atLeast(3)).executeEmptyR(captor.capture());
            List<String> allCalls = captor.getAllValues();
            String script = allCalls.get(allCalls.size() - 1);

            // Verify the if-conditional check to avoid duplicate vertices
            assertTrue(script.contains("if(length(as_ids(V(testGraph)"));
            assertTrue(script.contains("vertex_attr(testGraph, \"name\")"));
            assertTrue(script.contains("== 0)"));
        }

        @Test
        void addRelationship_vertexScript_containsNameValueType() {
            String[] headers = {"Title", "Genre"};
            Object[] values = {"Inception", "SciFi"};
            Map<Integer, Set<Integer>> cardinality = new HashMap<>();
            Set<Integer> ends = new HashSet<>();
            ends.add(1);
            cardinality.put(0, ends);

            graph.addRelationship(headers, values, cardinality);

            ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
            verify(mockTranslator, atLeast(3)).executeEmptyR(captor.capture());
            String script = captor.getAllValues().get(captor.getAllValues().size() - 1);

            // Verify vertex attributes: name, value, type
            assertTrue(script.contains("name=\"Title:Inception\""));
            assertTrue(script.contains("value=\"Inception\""));
            assertTrue(script.contains("type=\"Title\""));
        }
    }

    @Nested
    class UpsertEdgeSyntaxTests {
        @Test
        void addRelationship_edgeScript_containsConditionalCheck() {
            String[] headers = {"Title", "Genre"};
            Object[] values = {"Inception", "SciFi"};
            Map<Integer, Set<Integer>> cardinality = new HashMap<>();
            Set<Integer> ends = new HashSet<>();
            ends.add(1);
            cardinality.put(0, ends);

            graph.addRelationship(headers, values, cardinality);

            ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
            verify(mockTranslator, atLeast(3)).executeEmptyR(captor.capture());
            String script = captor.getAllValues().get(captor.getAllValues().size() - 1);

            // Verify edge conditional: avoid duplicates
            assertTrue(script.contains("if(length(as_ids(E(testGraph)"));
            assertTrue(script.contains("edge_attr(testGraph, \"name\")"));
        }

        @Test
        void addRelationship_edgeScript_containsFromAndToVertices() {
            String[] headers = {"Title", "Genre"};
            Object[] values = {"Inception", "SciFi"};
            Map<Integer, Set<Integer>> cardinality = new HashMap<>();
            Set<Integer> ends = new HashSet<>();
            ends.add(1);
            cardinality.put(0, ends);

            graph.addRelationship(headers, values, cardinality);

            ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
            verify(mockTranslator, atLeast(3)).executeEmptyR(captor.capture());
            String script = captor.getAllValues().get(captor.getAllValues().size() - 1);

            // Verify edge creation with correct vertex names
            assertTrue(script.contains("add_edges(testGraph"));
            assertTrue(script.contains("c(\"Title:Inception\", \"Genre:SciFi\")"));
            // Edge name is fromVertex:toVertex
            assertTrue(script.contains("name=\"Title:Inception:Genre:SciFi\""));
        }
    }

    @Nested
    class GraphNameTests {
        @Test
        void differentGraphName_usedInAllScripts() {
            AbstractRJavaTranslator translator = mock(AbstractRJavaTranslator.class);
            RiGraph g = new RiGraph("mySpecialGraph", translator);

            verify(translator).executeEmptyR("mySpecialGraph<- make_empty_graph()");

            when(translator.getInt("length(as_ids(mySpecialGraph))")).thenReturn(0);
            assertTrue(g.isEmpty());
        }

        @Test
        void differentGraphName_removeColumn() {
            AbstractRJavaTranslator translator = mock(AbstractRJavaTranslator.class);
            RiGraph g = new RiGraph("g1", translator);

            g.removeColumn("col1");

            String expected = "g1 <- delete_vertices(g1, V(g1)"
                    + "[vertex_attr(g1, \"type\") == \"col1\"])";
            verify(translator).executeEmptyR(expected);
        }
    }
}
