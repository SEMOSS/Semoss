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

class ModelInitListenerUnitTests {

    private static final String TEST_PATH = "/models/testModel/status";
    private ModelZKServer mockServer;
    private ZooKeeper mockZk;
    private ModelInitListener listener;

    @BeforeEach
    void setUp() {
        mockServer = mock(ModelZKServer.class);
        mockZk = mock(ZooKeeper.class);
        listener = new ModelInitListener(TEST_PATH, mockServer);
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
        ModelInitListener other = new ModelInitListener("/other/model/status", mockServer);
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
    void testProcessWithInitStatusAndCatchupFalseCallsSpinModel() {
        when(mockServer.getNodeData(TEST_PATH)).thenReturn("INIT");

        String modelNode = TEST_PATH.replace("/status", "");
        String smssValue = "model-smss-content";
        when(mockServer.getNodeData(modelNode)).thenReturn(smssValue);

        mockServer.catchup = false;

        listener.process(TEST_PATH, mockZk);

        verify(mockServer).spinModel(smssValue);
    }

    @Test
    void testProcessWithInitStatusCaseInsensitiveAndCatchupFalse() {
        when(mockServer.getNodeData(TEST_PATH)).thenReturn("init");

        String modelNode = TEST_PATH.replace("/status", "");
        String smssValue = "model-config";
        when(mockServer.getNodeData(modelNode)).thenReturn(smssValue);

        mockServer.catchup = false;

        listener.process(TEST_PATH, mockZk);

        verify(mockServer).spinModel(smssValue);
    }

    @Test
    void testProcessWithInitStatusAndCatchupTrueDoesNotCallSpinModel() {
        when(mockServer.getNodeData(TEST_PATH)).thenReturn("INIT");

        String modelNode = TEST_PATH.replace("/status", "");
        when(mockServer.getNodeData(modelNode)).thenReturn("model-smss");

        mockServer.catchup = true;

        listener.process(TEST_PATH, mockZk);

        verify(mockServer, never()).spinModel(anyString());
    }

    @Test
    void testProcessWithNonInitStatusDoesNotCallSpinModel() {
        when(mockServer.getNodeData(TEST_PATH)).thenReturn("Available");

        listener.process(TEST_PATH, mockZk);

        verify(mockServer, never()).spinModel(anyString());
    }

    @Test
    void testProcessWithFailStatusDoesNotCallSpinModel() {
        when(mockServer.getNodeData(TEST_PATH)).thenReturn("FAIL");

        listener.process(TEST_PATH, mockZk);

        verify(mockServer, never()).spinModel(anyString());
    }

    @Test
    void testProcessModelNodeIsPathWithoutStatus() {
        String pathWithStatus = "/cluster/models/myEngine/status";
        ModelInitListener initListener = new ModelInitListener(pathWithStatus, mockServer);

        when(mockServer.getNodeData(pathWithStatus)).thenReturn("INIT");
        when(mockServer.getNodeData("/cluster/models/myEngine")).thenReturn("my-smss");

        mockServer.catchup = false;

        initListener.process(pathWithStatus, mockZk);

        verify(mockServer).getNodeData("/cluster/models/myEngine");
        verify(mockServer).spinModel("my-smss");
    }

    @Test
    void testProcessWithRandomStatusAndCatchupFalseDoesNotCallSpinModel() {
        when(mockServer.getNodeData(TEST_PATH)).thenReturn("SomethingElse");

        mockServer.catchup = false;

        listener.process(TEST_PATH, mockZk);

        verify(mockServer, never()).spinModel(anyString());
    }
}
