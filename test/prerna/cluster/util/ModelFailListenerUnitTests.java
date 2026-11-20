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
package prerna.cluster.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;

import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ModelFailListenerUnitTests {

    private static final String TEST_PATH = "/models/testModel/fail";
    private ModelZKServer mockServer;
    private ZooKeeper mockZk;
    private ModelFailListener listener;

    @BeforeEach
    void setUp() {
        mockServer = mock(ModelZKServer.class);
        mockZk = mock(ZooKeeper.class);
        listener = new ModelFailListener(TEST_PATH, mockServer);
    }

    @Test
    void testConstructorSetsPathAndServer() {
        assertEquals(TEST_PATH, listener.getPath());
        assertNotNull(listener);
    }

    @Test
    void testGetPathReturnsPath() {
        assertEquals(TEST_PATH, listener.getPath());
    }

    @Test
    void testGetPathWithDifferentPath() {
        ModelFailListener other = new ModelFailListener("/other/path/fail", mockServer);
        assertEquals("/other/path/fail", other.getPath());
    }

    @Test
    void testGetEventsReturnsNodeChildrenChanged() {
        List<EventType> events = listener.getEvents();
        assertNotNull(events);
        assertEquals(1, events.size());
        assertEquals(EventType.NodeChildrenChanged, events.get(0));
    }

    @Test
    void testGetPredicatesReturnsEquals() {
        List<String> predicates = listener.getPredicates();
        assertNotNull(predicates);
        assertEquals(1, predicates.size());
        assertEquals("equals", predicates.get(0));
    }

    @Test
    void testSetModelZKUpdatesServer() {
        ModelZKServer newServer = mock(ModelZKServer.class);
        assertDoesNotThrow(() -> listener.setModelZK(newServer));
    }

    @Test
    void testProcessWhenFailCountEqualsServerCountCallsUpdateNodeData() throws Exception {
        List<String> failedChildren = List.of("server1", "server2", "server3");
        List<String> serverChildren = List.of("serverA", "serverB", "serverC");

        when(mockZk.getChildren(TEST_PATH, true)).thenReturn(failedChildren);
        when(mockZk.getChildren("/server", true)).thenReturn(serverChildren);

        listener.process(TEST_PATH, mockZk);

        String expectedStatusNode = TEST_PATH.replace("fail", "status");
        verify(mockServer).updateNodeData(expectedStatusNode, "FAIL", true);
    }

    @Test
    void testProcessWhenFailCountNotEqualServerCountDoesNotCallUpdateNodeData() throws Exception {
        List<String> failedChildren = List.of("server1");
        List<String> serverChildren = List.of("serverA", "serverB", "serverC");

        when(mockZk.getChildren(TEST_PATH, true)).thenReturn(failedChildren);
        when(mockZk.getChildren("/server", true)).thenReturn(serverChildren);

        listener.process(TEST_PATH, mockZk);

        verify(mockServer, never()).updateNodeData(anyString(), anyString(), anyBoolean());
    }

    @Test
    void testProcessStatusNodeReplacesFailWithStatus() throws Exception {
        String pathWithFail = "/models/myModel/fail";
        ModelFailListener failListener = new ModelFailListener(pathWithFail, mockServer);

        List<String> children = List.of("s1", "s2");
        when(mockZk.getChildren(pathWithFail, true)).thenReturn(children);
        when(mockZk.getChildren("/server", true)).thenReturn(children);

        failListener.process(pathWithFail, mockZk);

        verify(mockServer).updateNodeData("/models/myModel/status", "FAIL", true);
    }

    @Test
    void testProcessWithEmptyChildrenLists() throws Exception {
        List<String> emptyList = List.of();

        when(mockZk.getChildren(TEST_PATH, true)).thenReturn(emptyList);
        when(mockZk.getChildren("/server", true)).thenReturn(emptyList);

        listener.process(TEST_PATH, mockZk);

        String expectedStatusNode = TEST_PATH.replace("fail", "status");
        verify(mockServer).updateNodeData(expectedStatusNode, "FAIL", true);
    }

    @Test
    void testProcessWithMoreFailedThanServers() throws Exception {
        List<String> failedChildren = List.of("s1", "s2", "s3");
        List<String> serverChildren = List.of("s1", "s2");

        when(mockZk.getChildren(TEST_PATH, true)).thenReturn(failedChildren);
        when(mockZk.getChildren("/server", true)).thenReturn(serverChildren);

        listener.process(TEST_PATH, mockZk);

        verify(mockServer, never()).updateNodeData(anyString(), anyString(), anyBoolean());
    }
}
