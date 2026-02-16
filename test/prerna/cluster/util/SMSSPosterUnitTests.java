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
import static org.mockito.ArgumentMatchers.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SMSSPosterUnitTests {

    @Test void testProtectedConstructorCreatesInstance() throws Exception {
        Constructor<SMSSPoster> ctor = SMSSPoster.class.getDeclaredConstructor();
        ctor.setAccessible(true);
        assertNotNull(ctor.newInstance());
    }
    @Test void testConnectedDefaultsFalse() throws Exception {
        Constructor<SMSSPoster> ctor = SMSSPoster.class.getDeclaredConstructor(); ctor.setAccessible(true);
        SMSSPoster p = ctor.newInstance();
        Field f = SMSSPoster.class.getDeclaredField("connected"); f.setAccessible(true);
        assertFalse((boolean) f.get(p));
    }
    @Test void testModelIdDefaultsNull() throws Exception {
        Constructor<SMSSPoster> ctor = SMSSPoster.class.getDeclaredConstructor(); ctor.setAccessible(true);
        SMSSPoster p = ctor.newInstance();
        Field f = SMSSPoster.class.getDeclaredField("modelId"); f.setAccessible(true);
        assertNull(f.get(p));
    }
    @Test void testLockSetDefaultsFalse() throws Exception {
        Constructor<SMSSPoster> ctor = SMSSPoster.class.getDeclaredConstructor(); ctor.setAccessible(true);
        SMSSPoster p = ctor.newInstance();
        Field f = SMSSPoster.class.getDeclaredField("lockSet"); f.setAccessible(true);
        assertFalse((boolean) f.get(p));
    }
    @Test void testIsInstanceOfModelZKServer() throws Exception {
        Constructor<SMSSPoster> ctor = SMSSPoster.class.getDeclaredConstructor(); ctor.setAccessible(true);
        assertInstanceOf(ModelZKServer.class, ctor.newInstance());
    }
    @Test void testPosterSingletonFieldType() throws Exception {
        Field f = SMSSPoster.class.getDeclaredField("poster"); f.setAccessible(true);
        Object value = f.get(null);
        assertTrue(value == null || value instanceof SMSSPoster);
    }

    @Nested
    class InheritedConstantsTests {
        @Test void testZkServer() { assertEquals("zk", ModelZKServer.ZK_SERVER); }
        @Test void testHost() { assertEquals("host", ModelZKServer.HOST); }
        @Test void testTimeout() { assertEquals("to", ModelZKServer.TIMEOUT); }
        @Test void testBootuser() { assertEquals("bu", ModelZKServer.BOOTUSER); }
        @Test void testHome() { assertEquals("zk_home", ModelZKServer.HOME); }
        @Test void testAppHome() { assertEquals("app", ModelZKServer.APP_HOME); }
        @Test void testSemossHome() { assertEquals("sem", ModelZKServer.SEMOSS_HOME); }
        @Test void testModelRoot() { assertEquals("/model", ModelZKServer.MODEL_ROOT); }
        @Test void testServerPath() { assertEquals("/server", ModelZKServer.SERVER_PATH); }
    }

    @Nested
    class InheritedFieldDefaultsTests {
        private SMSSPoster poster;
        @BeforeEach void setUp() throws Exception {
            Constructor<SMSSPoster> ctor = SMSSPoster.class.getDeclaredConstructor(); ctor.setAccessible(true);
            poster = ctor.newInstance();
        }
        @Test void testDefaultZkServer() { assertEquals("localhost:2181", poster.zkServer); }
        @Test void testDefaultHost() { assertEquals("localhost:8888", poster.host); }
        @Test void testDefaultUser() { assertEquals("generic", poster.user); }
        @Test void testDefaultHome() { assertEquals("/semoss_root", poster.home); }
        @Test void testDefaultContainer() { assertEquals("/container", poster.container); }
        @Test void testDefaultApp() { assertEquals("/app", poster.app); }
        @Test void testDefaultCatchupTrue() { assertTrue(poster.catchup); }
        @Test void testDefaultInitFalse() { assertFalse(poster.init); }
        @Test void testDefaultId() { assertEquals("RANDOM_ID", poster.id); }
    }

    @Nested
    class MockZKTests {
        private SMSSPoster poster;
        private ZooKeeper mockZk;

        @BeforeEach void setUp() throws Exception {
            Constructor<SMSSPoster> ctor = SMSSPoster.class.getDeclaredConstructor(); ctor.setAccessible(true);
            poster = ctor.newInstance();
            mockZk = mock(ZooKeeper.class);
            poster.zk = mockZk;
            Field modelIdField = SMSSPoster.class.getDeclaredField("modelId");
            modelIdField.setAccessible(true);
            modelIdField.set(poster, "testModel");
        }

        @Test void testCheckServers_nonEmpty_returnsTrue() throws Exception {
            when(mockZk.getChildren(eq("/server"), eq(false))).thenReturn(Arrays.asList("server1", "server2"));
            assertTrue(poster.checkServers());
            verify(mockZk).getChildren(eq("/server"), eq(false));
        }
        @Test void testCheckServers_empty_throwsRuntime() throws Exception {
            when(mockZk.getChildren(eq("/server"), eq(false))).thenReturn(Collections.emptyList());
            assertThrows(RuntimeException.class, () -> poster.checkServers());
        }
        @Test void testCheckServers_keeperException_returnsFalse() throws Exception {
            when(mockZk.getChildren(eq("/server"), eq(false))).thenThrow(new KeeperException.ConnectionLossException());
            assertFalse(poster.checkServers());
        }
        @Test void testCheckServers_interruptedException_returnsFalse() throws Exception {
            when(mockZk.getChildren(eq("/server"), eq(false))).thenThrow(new InterruptedException("test"));
            assertFalse(poster.checkServers());
        }
        @Test void testDeleteNode_callsDeleteThreeTimes() throws Exception {
            poster.deleteNode();
            verify(mockZk).delete(eq(ModelZKServer.MODEL_ROOT + "/testModel/status"), eq(-1));
            verify(mockZk).delete(eq(ModelZKServer.MODEL_ROOT + "/testModel/fail"), eq(-1));
            verify(mockZk).delete(eq(ModelZKServer.MODEL_ROOT + "/testModel"), eq(-1));
        }
        @Test void testDeleteNode_verifyDeleteOrder() throws Exception {
            var inOrder = inOrder(mockZk);
            poster.deleteNode();
            inOrder.verify(mockZk).delete(eq("/model/testModel/status"), eq(-1));
            inOrder.verify(mockZk).delete(eq("/model/testModel/fail"), eq(-1));
            inOrder.verify(mockZk).delete(eq("/model/testModel"), eq(-1));
        }
        @Test void testDeleteNode_keeperException_doesNotThrow() throws Exception {
            doThrow(new KeeperException.NoNodeException()).when(mockZk).delete(anyString(), anyInt());
            assertDoesNotThrow(() -> poster.deleteNode());
        }
        @Test void testDeleteNode_withDifferentModelId() throws Exception {
            Field modelIdField = SMSSPoster.class.getDeclaredField("modelId");
            modelIdField.setAccessible(true);
            modelIdField.set(poster, "anotherModel");
            poster.deleteNode();
            verify(mockZk).delete(eq("/model/anotherModel/status"), eq(-1));
            verify(mockZk).delete(eq("/model/anotherModel/fail"), eq(-1));
            verify(mockZk).delete(eq("/model/anotherModel"), eq(-1));
        }
        @Test void testProcess_delegatesToProcessEvent() {
            IModelZKListener mockListener = mock(IModelZKListener.class);
            when(mockListener.getPath()).thenReturn("/model/test");
            when(mockListener.getEvents()).thenReturn(Arrays.asList(EventType.NodeDataChanged));
            poster.addZKListener(mockListener);
            poster.process(new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, "/model/test"));
            verify(mockListener).process("/model/test", mockZk);
        }
        @Test void testProcess_nullPath_doesNotThrow() {
            assertDoesNotThrow(() -> poster.process(new WatchedEvent(EventType.None, KeeperState.SyncConnected, null)));
        }
        @Test void testProcess_nonMatchingPath_listenerNotCalled() {
            IModelZKListener m = mock(IModelZKListener.class);
            when(m.getPath()).thenReturn("/model/test");
            when(m.getEvents()).thenReturn(Arrays.asList(EventType.NodeDataChanged));
            poster.addZKListener(m);
            poster.process(new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, "/model/other"));
            verify(m, never()).process(anyString(), any());
        }
        @Test void testCanAccomodate_inherited() {
            assertTrue(poster.canAccomodate(new java.util.HashMap<>(), new java.util.HashMap<>()));
        }
        @Test void testGetNodeData_inherited() throws Exception {
            when(mockZk.getData(eq("/test"), eq(true), any(org.apache.zookeeper.data.Stat.class)))
                .thenReturn("data".getBytes("UTF-8"));
            assertEquals("data", poster.getNodeData("/test"));
        }
        @Test void testGetChildren_inherited() throws Exception {
            when(mockZk.getChildren(eq("/server"), eq(true))).thenReturn(Arrays.asList("s1"));
            assertEquals(Arrays.asList("s1"), poster.getChildren("/server", true));
        }
        @Test void testAddZKListener_inherited() {
            IModelZKListener m = mock(IModelZKListener.class);
            when(m.getPath()).thenReturn("/model/x");
            poster.addZKListener(m);
            assertTrue(poster.listeners.containsKey("/model/x"));
        }
    }
}
