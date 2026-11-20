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

class ModelUnavailableListenerUnitTests {

    private static final String TEST_PATH = "/models/testModel/endpoint";
    private ModelZKServer mockServer;
    private ZooKeeper mockZk;
    private ModelUnavailableListener listener;

    @BeforeEach
    void setUp() {
        mockServer = mock(ModelZKServer.class);
        mockZk = mock(ZooKeeper.class);
        listener = new ModelUnavailableListener(TEST_PATH, mockServer);
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
        ModelUnavailableListener other = new ModelUnavailableListener("/other/endpoint", mockServer);
        assertEquals("/other/endpoint", other.getPath());
    }

    @Test
    void testGetEventsReturnsNodeDeleted() {
        List<EventType> events = listener.getEvents();
        assertNotNull(events);
        assertEquals(1, events.size());
        assertEquals(EventType.NodeDeleted, events.get(0));
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
    void testProcessReplacesEndpointWithStatusAndCallsUpdateNodeData() {
        listener.process(TEST_PATH, mockZk);

        String expectedStatusNode = TEST_PATH.replace("endpoint", "status");
        verify(mockServer).updateNodeData(expectedStatusNode, "INIT", true);
    }

    @Test
    void testProcessStatusNodePathCorrectness() {
        String endpointPath = "/cluster/models/myEngine/endpoint";
        ModelUnavailableListener unavailListener = new ModelUnavailableListener(endpointPath, mockServer);

        unavailListener.process(endpointPath, mockZk);

        verify(mockServer).updateNodeData("/cluster/models/myEngine/status", "INIT", true);
    }

    @Test
    void testProcessCallsUpdateNodeDataWithCorrectArguments() {
        listener.process(TEST_PATH, mockZk);

        String expectedStatusNode = TEST_PATH.replace("endpoint", "status");
        verify(mockServer).updateNodeData(
                eq(expectedStatusNode),
                eq("INIT"),
                eq(true)
        );
    }

    @Test
    void testProcessAfterSetModelZKUsesNewServer() {
        ModelZKServer newServer = mock(ModelZKServer.class);
        listener.setModelZK(newServer);

        listener.process(TEST_PATH, mockZk);

        String expectedStatusNode = TEST_PATH.replace("endpoint", "status");
        verify(newServer).updateNodeData(expectedStatusNode, "INIT", true);
        verify(mockServer, never()).updateNodeData(anyString(), anyString(), anyBoolean());
    }

    @Test
    void testProcessDoesNotInteractWithZooKeeper() {
        listener.process(TEST_PATH, mockZk);

        verifyNoInteractions(mockZk);
    }
}
