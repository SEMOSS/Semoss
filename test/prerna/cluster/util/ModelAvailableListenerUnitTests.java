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

class ModelAvailableListenerUnitTests {

    private static final String TEST_PATH = "/models/testModel/status";
    private ModelZKServer mockServer;
    private ZooKeeper mockZk;
    private ModelAvailableListener listener;

    @BeforeEach
    void setUp() {
        mockServer = mock(ModelZKServer.class);
        mockZk = mock(ZooKeeper.class);
        listener = new ModelAvailableListener(TEST_PATH, mockServer);
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
        ModelAvailableListener other = new ModelAvailableListener("/other/model/status", mockServer);
        assertEquals("/other/model/status", other.getPath());
    }

    @Test
    void testGetEventsReturnsNodeDataChanged() {
        List<EventType> events = listener.getEvents();
        assertNotNull(events);
        assertEquals(1, events.size());
        assertEquals(EventType.NodeDataChanged, events.get(0));
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
    void testAvailableConstant() {
        assertEquals("Available", ModelAvailableListener.AVAILABLE);
    }

    @Test
    void testProcessWithAvailableStatusCallsGetNodeDataOnEndpoint() {
        when(mockServer.getNodeData(TEST_PATH)).thenReturn("Available");

        String expectedEndpointNode = TEST_PATH.replace("status", "endpoint");
        when(mockServer.getNodeData(expectedEndpointNode)).thenReturn("http://localhost:8080");

        listener.process(TEST_PATH, mockZk);

        verify(mockServer).getNodeData(TEST_PATH);
        verify(mockServer).getNodeData(expectedEndpointNode);
    }

    @Test
    void testProcessWithAvailableStatusCaseInsensitive() {
        when(mockServer.getNodeData(TEST_PATH)).thenReturn("available");

        String expectedEndpointNode = TEST_PATH.replace("status", "endpoint");
        when(mockServer.getNodeData(expectedEndpointNode)).thenReturn("http://localhost:9090");

        listener.process(TEST_PATH, mockZk);

        verify(mockServer).getNodeData(TEST_PATH);
        verify(mockServer).getNodeData(expectedEndpointNode);
    }

    @Test
    void testProcessWithFailStatusDoesNotCrash() {
        when(mockServer.getNodeData(TEST_PATH)).thenReturn("Fail");

        assertDoesNotThrow(() -> listener.process(TEST_PATH, mockZk));
        verify(mockServer).getNodeData(TEST_PATH);
    }

    @Test
    void testProcessWithFailStatusCaseInsensitive() {
        when(mockServer.getNodeData(TEST_PATH)).thenReturn("fail");

        assertDoesNotThrow(() -> listener.process(TEST_PATH, mockZk));
        verify(mockServer).getNodeData(TEST_PATH);
    }

    @Test
    void testProcessWithOtherStatusDoesNothingExtra() {
        when(mockServer.getNodeData(TEST_PATH)).thenReturn("SomeOtherStatus");

        listener.process(TEST_PATH, mockZk);

        // Only the initial getNodeData for the status should be called
        verify(mockServer, times(1)).getNodeData(TEST_PATH);
        verifyNoMoreInteractions(mockServer);
    }

    @Test
    void testProcessWithInitStatusDoesNothingExtra() {
        when(mockServer.getNodeData(TEST_PATH)).thenReturn("INIT");

        listener.process(TEST_PATH, mockZk);

        verify(mockServer, times(1)).getNodeData(TEST_PATH);
        verifyNoMoreInteractions(mockServer);
    }

    @Test
    void testProcessEndpointNodeReplacesStatusWithEndpoint() {
        String statusPath = "/models/myModel/status";
        ModelAvailableListener avListener = new ModelAvailableListener(statusPath, mockServer);

        when(mockServer.getNodeData(statusPath)).thenReturn("Available");
        when(mockServer.getNodeData("/models/myModel/endpoint")).thenReturn("http://host:1234");

        avListener.process(statusPath, mockZk);

        verify(mockServer).getNodeData("/models/myModel/endpoint");
    }
}
