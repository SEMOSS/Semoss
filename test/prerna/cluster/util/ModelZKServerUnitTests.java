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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.Test;
import com.google.gson.Gson;

class ModelZKServerUnitTests {

    private ModelZKServer server;

    @BeforeEach
    void setUp() throws Exception {
        Constructor<ModelZKServer> ctor = ModelZKServer.class.getDeclaredConstructor();
        ctor.setAccessible(true);
        server = ctor.newInstance();
    }

    @Nested
    class ConstantsTests {
        @Test void testZkServerConstant() { assertEquals("zk", ModelZKServer.ZK_SERVER); }
        @Test void testHostConstant() { assertEquals("host", ModelZKServer.HOST); }
        @Test void testTimeoutConstant() { assertEquals("to", ModelZKServer.TIMEOUT); }
        @Test void testBootuserConstant() { assertEquals("bu", ModelZKServer.BOOTUSER); }
        @Test void testHomeConstant() { assertEquals("zk_home", ModelZKServer.HOME); }
        @Test void testAppHomeConstant() { assertEquals("app", ModelZKServer.APP_HOME); }
        @Test void testSemossHomeConstant() { assertEquals("sem", ModelZKServer.SEMOSS_HOME); }
        @Test void testAvailableConstant() { assertEquals("available", ModelZKServer.AVAILABLE); }
        @Test void testModelRootConstant() { assertEquals("/model", ModelZKServer.MODEL_ROOT); }
        @Test void testServerPathConstant() { assertEquals("/server", ModelZKServer.SERVER_PATH); }
    }

    @Nested
    class DefaultFieldValuesTests {
        @Test void testDefaultZkServer() { assertEquals("localhost:2181", server.zkServer); }
        @Test void testDefaultHost() { assertEquals("localhost:8888", server.host); }
        @Test void testDefaultUser() { assertEquals("generic", server.user); }
        @Test void testDefaultHome() { assertEquals("/semoss_root", server.home); }
        @Test void testDefaultApp() { assertEquals("/app", server.app); }
        @Test void testDefaultSemossHome() { assertEquals("/opt/semosshome/", ModelZKServer.semossHome); }
        @Test void testDefaultContainer() { assertEquals("/container", server.container); }
        @Test void testDefaultId() { assertEquals("RANDOM_ID", server.id); }
        @Test void testDefaultCatchupTrue() { assertTrue(server.catchup); }
        @Test void testDefaultInitFalse() { assertFalse(server.init); }
        @Test void testDefaultPropNull() { assertNull(server.prop); }
        @Test void testDefaultZkNull() { assertNull(server.zk); }
        @Test void testDefaultEnvNull() { assertNull(server.env); }
        @Test void testDefaultListenersEmpty() { assertNotNull(server.listeners); assertTrue(server.listeners.isEmpty()); }
        @Test void testDefaultConnectedFalse() throws Exception {
            Field f = ModelZKServer.class.getDeclaredField("connected"); f.setAccessible(true); assertFalse((boolean) f.get(server));
        }
        @Test void testDefaultGsonNonNull() throws Exception {
            Field gsonField = ModelZKServer.class.getDeclaredField("gson"); gsonField.setAccessible(true);
            assertNotNull(gsonField.get(server)); assertInstanceOf(Gson.class, gsonField.get(server));
        }
        @Test void testDefaultExistingModelsEmpty() throws Exception {
            Field f = ModelZKServer.class.getDeclaredField("existingModels"); f.setAccessible(true);
            List<?> m = (List<?>) f.get(server); assertNotNull(m); assertTrue(m.isEmpty());
        }
        @Test void testDefaultSupportedModelsEmpty() throws Exception {
            Field f = ModelZKServer.class.getDeclaredField("supportedModels"); f.setAccessible(true);
            List<?> m = (List<?>) f.get(server); assertNotNull(m); assertTrue(m.isEmpty());
        }
        @Test void testDefaultModelSMSSEmpty() throws Exception {
            Field f = ModelZKServer.class.getDeclaredField("modelSMSS"); f.setAccessible(true);
            Map<?, ?> m = (Map<?, ?>) f.get(server); assertNotNull(m); assertTrue(m.isEmpty());
        }
        @Test void testDefaultModelLockEmpty() throws Exception {
            Field f = ModelZKServer.class.getDeclaredField("modelLock"); f.setAccessible(true);
            Map<?, ?> m = (Map<?, ?>) f.get(server); assertNotNull(m); assertTrue(m.isEmpty());
        }
        @Test void testDefaultModelClientEmpty() throws Exception {
            Field f = ModelZKServer.class.getDeclaredField("modelClient"); f.setAccessible(true);
            Map<?, ?> m = (Map<?, ?>) f.get(server); assertNotNull(m); assertTrue(m.isEmpty());
        }
        @Test void testDefaultRepeatEmpty() throws Exception {
            Field f = ModelZKServer.class.getDeclaredField("repeat"); f.setAccessible(true);
            Map<?, ?> m = (Map<?, ?>) f.get(server); assertNotNull(m); assertTrue(m.isEmpty());
        }
        @Test void testDefaultClientNull() throws Exception {
            Field f = ModelZKServer.class.getDeclaredField("client"); f.setAccessible(true); assertNull(f.get(server));
        }
        @Test void testDefaultVersionZero() throws Exception {
            Field f = ModelZKServer.class.getDeclaredField("version"); f.setAccessible(true); assertEquals(0, f.getInt(server));
        }
        @Test void testDefaultZkClientStaticFieldType() {
            try { Field f = ModelZKServer.class.getDeclaredField("zkClient"); assertEquals(ModelZKServer.class, f.getType()); }
            catch (NoSuchFieldException e) { fail("zkClient field should exist"); }
        }
    }

    @Nested
    class CanAccomodateTests {
        @Test void testBothMapsEmpty_returnsTrue() { assertTrue(server.canAccomodate(new HashMap<>(), new HashMap<>())); }
        @Test void testNeedSatisfied_returnsTrue() {
            Map<String, Object> cap = new HashMap<>(); Map<String, Object> sub = new HashMap<>();
            sub.put("available", 8.0); cap.put("gpu", sub);
            assertTrue(server.canAccomodate(cap, Map.of("gpu", 4)));
        }
        @Test void testNeedNotSatisfied_returnsFalse() {
            Map<String, Object> cap = new HashMap<>(); Map<String, Object> sub = new HashMap<>();
            sub.put("available", 5.0); cap.put("gpu", sub);
            assertFalse(server.canAccomodate(cap, Map.of("gpu", 10)));
        }
        @Test void testCapValueIsNotAMap_returnsFalse() {
            Map<String, Object> cap = new HashMap<>(); cap.put("gpu", "notAMap");
            assertFalse(server.canAccomodate(cap, Map.of("gpu", 4)));
        }
        @Test void testNeedKeyMissingFromCap_returnsFalse() {
            assertFalse(server.canAccomodate(new HashMap<>(), Map.of("gpu", 4)));
        }
        @Test void testNeedExactlyEqualsCapability_returnsTrue() {
            Map<String, Object> cap = new HashMap<>(); Map<String, Object> sub = new HashMap<>();
            sub.put("available", 4.0); cap.put("gpu", sub);
            assertTrue(server.canAccomodate(cap, Map.of("gpu", 4)));
        }
        @Test void testMultipleNeedsAllSatisfied_returnsTrue() {
            Map<String, Object> cap = new HashMap<>();
            Map<String, Object> gs = new HashMap<>(); gs.put("available", 8.0); cap.put("gpu", gs);
            Map<String, Object> ms = new HashMap<>(); ms.put("available", 32.0); cap.put("memory", ms);
            Map<String, Object> need = new HashMap<>(); need.put("gpu", 4); need.put("memory", 16);
            assertTrue(server.canAccomodate(cap, need));
        }
        @Test void testMultipleNeeds_oneFails_returnsFalse() {
            Map<String, Object> cap = new HashMap<>();
            Map<String, Object> gs = new HashMap<>(); gs.put("available", 8.0); cap.put("gpu", gs);
            Map<String, Object> ms = new HashMap<>(); ms.put("available", 10.0); cap.put("memory", ms);
            Map<String, Object> need = new HashMap<>(); need.put("gpu", 4); need.put("memory", 16);
            assertFalse(server.canAccomodate(cap, need));
        }
    }

    @Nested
    class ListenerTests {
        @Test void testAddZKListener_addsToListenersMap() {
            IModelZKListener m = mock(IModelZKListener.class); when(m.getPath()).thenReturn("/model/test");
            server.addZKListener(m);
            assertTrue(server.listeners.containsKey("/model/test"));
            assertEquals(1, server.listeners.get("/model/test").size());
            assertSame(m, server.listeners.get("/model/test").get(0));
        }
        @Test void testAddZKListener_multipleListenersSamePath() {
            IModelZKListener l1 = mock(IModelZKListener.class); when(l1.getPath()).thenReturn("/model/test");
            IModelZKListener l2 = mock(IModelZKListener.class); when(l2.getPath()).thenReturn("/model/test");
            server.addZKListener(l1); server.addZKListener(l2);
            assertEquals(2, server.listeners.get("/model/test").size());
        }
        @Test void testAddZKListener_differentPaths() {
            IModelZKListener l1 = mock(IModelZKListener.class); when(l1.getPath()).thenReturn("/model/t1");
            IModelZKListener l2 = mock(IModelZKListener.class); when(l2.getPath()).thenReturn("/model/t2");
            server.addZKListener(l1); server.addZKListener(l2);
            assertTrue(server.listeners.containsKey("/model/t1"));
            assertTrue(server.listeners.containsKey("/model/t2"));
        }
        @Test void testProcessEvent_matching() {
            IModelZKListener m = mock(IModelZKListener.class); when(m.getPath()).thenReturn("/model/test");
            when(m.getEvents()).thenReturn(Arrays.asList(EventType.NodeDataChanged));
            server.addZKListener(m);
            server.processEvent("/model/test", new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, "/model/test"));
            verify(m).process("/model/test", null);
        }
        @Test void testProcessEvent_nonMatchingPath() {
            IModelZKListener m = mock(IModelZKListener.class); when(m.getPath()).thenReturn("/model/test");
            when(m.getEvents()).thenReturn(Arrays.asList(EventType.NodeDataChanged));
            server.addZKListener(m);
            server.processEvent("/model/other", new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, "/model/other"));
            verify(m, never()).process(anyString(), any());
        }
        @Test void testProcessEvent_wrongEvent() {
            IModelZKListener m = mock(IModelZKListener.class); when(m.getPath()).thenReturn("/model/test");
            when(m.getEvents()).thenReturn(Arrays.asList(EventType.NodeCreated));
            server.addZKListener(m);
            server.processEvent("/model/test", new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, "/model/test"));
            verify(m, never()).process(anyString(), any());
        }
        @Test void testProcessEvent_noListeners() {
            assertDoesNotThrow(() -> server.processEvent("/model/unknown",
                new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, "/model/unknown")));
        }
        @Test void testProcessEvent_multipleListeners() {
            IModelZKListener l1 = mock(IModelZKListener.class); when(l1.getPath()).thenReturn("/model/test");
            IModelZKListener l2 = mock(IModelZKListener.class); when(l2.getPath()).thenReturn("/model/test");
            when(l1.getEvents()).thenReturn(Arrays.asList(EventType.NodeDataChanged));
            when(l2.getEvents()).thenReturn(Arrays.asList(EventType.NodeDataChanged));
            server.addZKListener(l1); server.addZKListener(l2);
            server.processEvent("/model/test", new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, "/model/test"));
            verify(l1).process("/model/test", null); verify(l2).process("/model/test", null);
        }
        @Test void testProcessEvent_multipleEventTypes() {
            IModelZKListener m = mock(IModelZKListener.class); when(m.getPath()).thenReturn("/model/test");
            when(m.getEvents()).thenReturn(Arrays.asList(EventType.NodeCreated, EventType.NodeDeleted));
            server.addZKListener(m);
            server.processEvent("/model/test", new WatchedEvent(EventType.NodeDeleted, KeeperState.SyncConnected, "/model/test"));
            verify(m).process("/model/test", null);
        }
    }

    @Nested
    class MockZKTests {
        private ZooKeeper mockZk;
        @BeforeEach void setUpMockZk() { mockZk = mock(ZooKeeper.class); server.zk = mockZk; }
        @Test void testGetNodeData_returnsStringFromBytes() throws Exception {
            when(mockZk.getData(eq("/model/test"), eq(true), any(Stat.class))).thenReturn("hello world".getBytes("UTF-8"));
            assertEquals("hello world", server.getNodeData("/model/test"));
            verify(mockZk).getData(eq("/model/test"), eq(true), any(Stat.class));
        }
        @Test void testGetNodeData_exceptionReturnsNull() throws Exception {
            when(mockZk.getData(eq("/model/test"), eq(true), any(Stat.class))).thenThrow(new KeeperException.NoNodeException());
            assertNull(server.getNodeData("/model/test"));
        }
        @Test void testGetNodeData_emptyBytes() throws Exception {
            when(mockZk.getData(eq("/model/empty"), eq(true), any(Stat.class))).thenReturn(new byte[0]);
            assertEquals("", server.getNodeData("/model/empty"));
        }
        @Test void testUpdateNodeData_createTrue_nodeDoesNotExist() throws Exception {
            when(mockZk.exists(eq("/model/test"), eq(false))).thenReturn(null);
            server.updateNodeData("/model/test", "data", true);
            verify(mockZk).create(eq("/model/test"), eq("data".getBytes()), eq(Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL), any(Stat.class));
            verify(mockZk, never()).setData(anyString(), any(byte[].class), anyInt());
        }
        @Test void testUpdateNodeData_createFalse_doesNothing() throws Exception {
            when(mockZk.exists(eq("/model/test"), eq(false))).thenReturn(null);
            server.updateNodeData("/model/test", "data", false);
            verify(mockZk, never()).create(anyString(), any(byte[].class), anyList(), any(CreateMode.class), any(Stat.class));
            verify(mockZk, never()).setData(anyString(), any(byte[].class), anyInt());
        }
        @Test void testUpdateNodeData_nodeExists_callsSetData() throws Exception {
            when(mockZk.exists(eq("/model/test"), eq(false))).thenReturn(new Stat());
            server.updateNodeData("/model/test", "updated", true);
            verify(mockZk).setData(eq("/model/test"), eq("updated".getBytes()), eq(-1));
        }
        @Test void testUpdateNodeData_keeperException() throws Exception {
            when(mockZk.exists(eq("/model/test"), eq(false))).thenThrow(new KeeperException.ConnectionLossException());
            assertDoesNotThrow(() -> server.updateNodeData("/model/test", "data", true));
        }
        @Test void testGetChildren_watchTrue() throws Exception {
            List<String> expected = Arrays.asList("child1", "child2");
            when(mockZk.getChildren(eq("/model"), eq(true))).thenReturn(expected);
            assertEquals(expected, server.getChildren("/model", true));
        }
        @Test void testGetChildren_watchFalse() throws Exception {
            when(mockZk.getChildren(eq("/server"), eq(false))).thenReturn(Arrays.asList("a", "b", "c"));
            assertEquals(Arrays.asList("a", "b", "c"), server.getChildren("/server", false));
        }
        @Test void testGetChildren_keeperException() throws Exception {
            when(mockZk.getChildren(eq("/model"), eq(true))).thenThrow(new KeeperException.ConnectionLossException());
            assertThrows(IllegalStateException.class, () -> server.getChildren("/model", true));
        }
        @Test void testGetChildren_interruptedException() throws Exception {
            when(mockZk.getChildren(eq("/model"), eq(true))).thenThrow(new InterruptedException("test"));
            assertThrows(IllegalStateException.class, () -> server.getChildren("/model", true));
        }
        @Test void testGetChildren_emptyList() throws Exception {
            when(mockZk.getChildren(eq("/model"), eq(false))).thenReturn(Arrays.asList());
            assertTrue(server.getChildren("/model", false).isEmpty());
        }
        @Test void testAddServer_pathDoesNotExist() throws Exception {
            when(mockZk.exists(eq(ModelZKServer.SERVER_PATH), eq(false))).thenReturn(null);
            when(mockZk.create(anyString(), any(byte[].class), anyList(), any(CreateMode.class))).thenReturn("");
            server.addServer();
            verify(mockZk).create(eq(ModelZKServer.SERVER_PATH), eq("1".getBytes()), eq(Ids.OPEN_ACL_UNSAFE), eq(CreateMode.PERSISTENT));
            verify(mockZk).create(eq(ModelZKServer.SERVER_PATH + "/" + server.id), eq(server.id.getBytes()), eq(Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL));
        }
        @Test void testAddServer_pathExists() throws Exception {
            when(mockZk.exists(eq(ModelZKServer.SERVER_PATH), eq(false))).thenReturn(new Stat());
            when(mockZk.create(anyString(), any(byte[].class), anyList(), any(CreateMode.class))).thenReturn("");
            server.addServer();
            verify(mockZk, never()).create(eq(ModelZKServer.SERVER_PATH), any(byte[].class), anyList(), eq(CreateMode.PERSISTENT));
            verify(mockZk).create(eq(ModelZKServer.SERVER_PATH + "/" + server.id), eq(server.id.getBytes()), eq(Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL));
        }
        @Test void testAddServer_keeperException() throws Exception {
            when(mockZk.exists(eq(ModelZKServer.SERVER_PATH), eq(false))).thenThrow(new KeeperException.ConnectionLossException());
            assertDoesNotThrow(() -> server.addServer());
        }
        @Test void testProcess_delegatesToProcessEvent() {
            IModelZKListener m = mock(IModelZKListener.class); when(m.getPath()).thenReturn("/model/test");
            when(m.getEvents()).thenReturn(Arrays.asList(EventType.NodeDataChanged));
            server.addZKListener(m);
            server.process(new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, "/model/test"));
            verify(m).process("/model/test", mockZk);
        }
        @Test void testProcess_nullPath() {
            assertDoesNotThrow(() -> server.process(new WatchedEvent(EventType.None, KeeperState.SyncConnected, null)));
        }
        @Test void testProcess_nodeCreated() {
            IModelZKListener m = mock(IModelZKListener.class); when(m.getPath()).thenReturn("/model/new");
            when(m.getEvents()).thenReturn(Arrays.asList(EventType.NodeCreated));
            server.addZKListener(m);
            server.process(new WatchedEvent(EventType.NodeCreated, KeeperState.SyncConnected, "/model/new"));
            verify(m).process("/model/new", mockZk);
        }
        @Test void testProcess_nodeDeleted() {
            IModelZKListener m = mock(IModelZKListener.class); when(m.getPath()).thenReturn("/model/gone");
            when(m.getEvents()).thenReturn(Arrays.asList(EventType.NodeDeleted));
            server.addZKListener(m);
            server.process(new WatchedEvent(EventType.NodeDeleted, KeeperState.SyncConnected, "/model/gone"));
            verify(m).process("/model/gone", mockZk);
        }
        @Test void testWatch4Data_callsGetData() throws Exception {
            when(mockZk.getData(eq("/model/test"), eq(true), any(Stat.class))).thenReturn("data".getBytes("UTF-8"));
            server.watch4Data("/model/test");
            verify(mockZk).getData(eq("/model/test"), eq(true), any(Stat.class));
        }
        @Test void testWatch4Data_keeperException() throws Exception {
            when(mockZk.getData(eq("/model/m"), eq(true), any(Stat.class))).thenThrow(new KeeperException.NoNodeException());
            assertDoesNotThrow(() -> server.watch4Data("/model/m"));
        }
        @Test void testWatch4Data_interruptedException() throws Exception {
            when(mockZk.getData(eq("/model/t"), eq(true), any(Stat.class))).thenThrow(new InterruptedException("t"));
            assertDoesNotThrow(() -> server.watch4Data("/model/t"));
        }
        @Test void testWatch4Children_callsGetChildren() throws Exception {
            when(mockZk.getChildren(eq("/model"), eq(true))).thenReturn(Arrays.asList("c1"));
            server.watch4Children("/model");
            verify(mockZk).getChildren(eq("/model"), eq(true));
        }
        @Test void testWatch4Children_keeperException() throws Exception {
            when(mockZk.getChildren(eq("/model/m"), eq(true))).thenThrow(new KeeperException.NoNodeException());
            assertDoesNotThrow(() -> server.watch4Children("/model/m"));
        }
        @Test void testWatch4Children_interruptedException() throws Exception {
            when(mockZk.getChildren(eq("/model/t"), eq(true))).thenThrow(new InterruptedException("t"));
            assertDoesNotThrow(() -> server.watch4Children("/model/t"));
        }
        @Test void testReconnect_whenZkClientIsNull() throws Exception {
            Field zkClientField = ModelZKServer.class.getDeclaredField("zkClient");
            zkClientField.setAccessible(true);
            Object original = zkClientField.get(null);
            try { zkClientField.set(null, null); assertThrows(NullPointerException.class, () -> server.reconnect()); }
            finally { zkClientField.set(null, original); }
        }
    }
}
