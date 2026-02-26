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

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ZKClientUnitTests {

    private ZKClient client;
    private ZooKeeper mockZk;

    @BeforeEach
    void setUp() throws Exception {
        Constructor<ZKClient> ctor = ZKClient.class.getDeclaredConstructor();
        ctor.setAccessible(true);
        client = ctor.newInstance();
        mockZk = mock(ZooKeeper.class);
        client.zk = mockZk;
    }

    @Test
    void testZkServerConstant() {
        assertEquals("zk", ZKClient.ZK_SERVER);
    }

    @Test
    void testHostConstant() {
        assertEquals("host", ZKClient.HOST);
    }

    @Test
    void testTimeoutConstant() {
        assertEquals("to", ZKClient.TIMEOUT);
    }

    @Test
    void testBootuserConstant() {
        assertEquals("bu", ZKClient.BOOTUSER);
    }

    @Test
    void testHomeConstant() {
        assertEquals("zk_home", ZKClient.HOME);
    }

    @Test
    void testAppHomeConstant() {
        assertEquals("app", ZKClient.APP_HOME);
    }

    @Test
    void testSemossHomeConstant() {
        assertEquals("sem", ZKClient.SEMOSS_HOME);
    }

    @Test
    void testDefaultFieldValues() {
        assertEquals("localhost:2181", client.zkServer);
        assertEquals("localhost:8888", client.host);
        assertEquals("generic", client.user);
        assertEquals("/semoss_root", client.home);
        assertEquals("/container", client.container);
        assertEquals("/app", client.app);
    }

    @Test
    void testGetPayload_containsExpectedKeys() {
        String payload = client.getPayload();
        assertTrue(payload.contains("cpu="));
        assertTrue(payload.contains("memory="));
        assertTrue(payload.contains("rver=3.5"));
        assertTrue(payload.contains("semoss=3.5"));
        assertTrue(payload.contains("url="));
        assertTrue(payload.contains("user"));
    }

    @Test
    void testGetPayload_usesHostAndUser() {
        String payload = client.getPayload();
        assertTrue(payload.contains("url=localhost:8888"));
        assertTrue(payload.contains("usergeneric"));
    }

    @Test
    void testGetPayload_withCustomValues() {
        client.host = "10.0.0.1:9090";
        client.user = "admin";
        String payload = client.getPayload();
        assertTrue(payload.contains("url=10.0.0.1:9090"));
        assertTrue(payload.contains("useradmin"));
    }

    @Test
    void testGetPayload_pipeDelimited() {
        String payload = client.getPayload();
        String[] parts = payload.split("\\|");
        assertTrue(parts.length >= 6, "Should have at least 6 pipe-separated sections");
    }

    @Test
    void testGetPayload_cpuIsPositive() {
        String payload = client.getPayload();
        int cpuStart = payload.indexOf("cpu=") + 4;
        int cpuEnd = payload.indexOf("|", cpuStart);
        int cpus = Integer.parseInt(payload.substring(cpuStart, cpuEnd));
        assertTrue(cpus > 0, "CPU count should be positive");
    }

    @Test
    void testPublishNode_callsZkCreateWithCorrectPath() throws Exception {
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL_SEQUENTIAL)))
                .thenReturn("/semoss_root/semoss0000000001");
        when(mockZk.setData(anyString(), any(byte[].class), eq(-1))).thenReturn(new Stat());
        client.publishNode();
        verify(mockZk).create(eq("/semoss_root/semoss"), any(byte[].class),
                eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL_SEQUENTIAL));
    }

    @Test
    void testPublishNode_payloadMatchesGetPayload() throws Exception {
        String expectedPayload = client.getPayload();
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL_SEQUENTIAL)))
                .thenReturn("/semoss_root/semoss0000000001");
        when(mockZk.setData(anyString(), any(byte[].class), eq(-1))).thenReturn(new Stat());
        client.publishNode();
        verify(mockZk).create(eq("/semoss_root/semoss"), eq(expectedPayload.getBytes()),
                eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL_SEQUENTIAL));
    }

    @Test
    void testPublishNode_callsTouchRoot() throws Exception {
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL_SEQUENTIAL)))
                .thenReturn("/semoss_root/semoss0000000001");
        when(mockZk.setData(anyString(), any(byte[].class), eq(-1))).thenReturn(new Stat());
        client.publishNode();
        verify(mockZk).setData(eq("/semoss_root"), any(byte[].class), eq(-1));
    }

    @Test
    void testPublishNode_withCustomHome() throws Exception {
        client.home = "/custom_root";
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL_SEQUENTIAL)))
                .thenReturn("/custom_root/semoss0000000001");
        when(mockZk.setData(anyString(), any(byte[].class), eq(-1))).thenReturn(new Stat());
        client.publishNode();
        verify(mockZk).create(eq("/custom_root/semoss"), any(byte[].class),
                eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL_SEQUENTIAL));
    }

    @Test
    void testPublishDB_callsZkCreateWithCorrectPath() throws Exception {
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL)))
                .thenReturn("/semoss_root/app/myEngine");
        client.publishDB("myEngine");
        verify(mockZk).create(eq("/semoss_root/app/myEngine"), any(byte[].class),
                eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL));
    }

    @Test
    void testPublishDB_sendsHostAsPayload() throws Exception {
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL)))
                .thenReturn("/semoss_root/app/engine1");
        client.publishDB("engine1");
        verify(mockZk).create(eq("/semoss_root/app/engine1"), eq("localhost:8888".getBytes()),
                eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL));
    }

    @Test
    void testPublishDB_withCustomHostAndApp() throws Exception {
        client.host = "10.0.0.5:7777";
        client.app = "/databases";
        client.home = "/root";
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL)))
                .thenReturn("/root/databases/eng123");
        client.publishDB("eng123");
        verify(mockZk).create(eq("/root/databases/eng123"), eq("10.0.0.5:7777".getBytes()),
                eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL));
    }

    @Test
    void testPublishDB_doesNotCallTouchRoot() throws Exception {
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL)))
                .thenReturn("/semoss_root/app/myEngine");
        client.publishDB("myEngine");
        verify(mockZk, never()).setData(anyString(), any(byte[].class), anyInt());
    }

    @Test
    void testDeleteDB_callsZkDeleteWithCorrectPath() throws Exception {
        client.deleteDB("myEngine");
        verify(mockZk).delete(eq("/semoss_root/app/myEngine"), eq(-1));
    }

    @Test
    void testDeleteDB_withCustomHomeAndApp() throws Exception {
        client.home = "/cluster";
        client.app = "/engines";
        client.deleteDB("engine42");
        verify(mockZk).delete(eq("/cluster/engines/engine42"), eq(-1));
    }

    @Test
    void testDeleteDB_usesVersionNegativeOne() throws Exception {
        client.deleteDB("testDB");
        verify(mockZk).delete(eq("/semoss_root/app/testDB"), eq(-1));
    }

    @Test
    void testPublishContainer_callsZkCreateWithCorrectPath() throws Exception {
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL)))
                .thenReturn("/semoss_root/container/192.168.1.1:8080");
        when(mockZk.setData(anyString(), any(byte[].class), eq(-1))).thenReturn(new Stat());
        client.publishContainer("192.168.1.1:8080");
        verify(mockZk).create(eq("/semoss_root/container/192.168.1.1:8080"), any(byte[].class),
                eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL));
    }

    @Test
    void testPublishContainer_sendsHostAsPayload() throws Exception {
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL)))
                .thenReturn("/semoss_root/container/10.0.0.1:9090");
        when(mockZk.setData(anyString(), any(byte[].class), eq(-1))).thenReturn(new Stat());
        client.publishContainer("10.0.0.1:9090");
        verify(mockZk).create(eq("/semoss_root/container/10.0.0.1:9090"), eq("localhost:8888".getBytes()),
                eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL));
    }

    @Test
    void testPublishContainer_callsTouchRoot() throws Exception {
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL)))
                .thenReturn("/semoss_root/container/host:8080");
        when(mockZk.setData(anyString(), any(byte[].class), eq(-1))).thenReturn(new Stat());
        client.publishContainer("host:8080");
        verify(mockZk).setData(eq("/semoss_root"), any(byte[].class), eq(-1));
    }

    @Test
    void testPublishContainer_withCustomContainer() throws Exception {
        client.container = "/nodes";
        client.home = "/root";
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL)))
                .thenReturn("/root/nodes/host:8080");
        when(mockZk.setData(anyString(), any(byte[].class), eq(-1))).thenReturn(new Stat());
        client.publishContainer("host:8080");
        verify(mockZk).create(eq("/root/nodes/host:8080"), any(byte[].class),
                eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL));
    }

    @Test
    void testDeleteContainer_callsZkDeleteWithCorrectPath() throws Exception {
        client.deleteContainer("192.168.1.1:8080");
        verify(mockZk).delete(eq("/semoss_root/container/192.168.1.1:8080"), eq(-1));
    }

    @Test
    void testDeleteContainer_withCustomHomeAndContainer() throws Exception {
        client.home = "/myroot";
        client.container = "/nodes";
        client.deleteContainer("host:9090");
        verify(mockZk).delete(eq("/myroot/nodes/host:9090"), eq(-1));
    }

    @Test
    void testDeleteContainer_usesVersionNegativeOne() throws Exception {
        client.deleteContainer("test:1234");
        verify(mockZk).delete(eq("/semoss_root/container/test:1234"), eq(-1));
    }

    @Test
    void testGetHostForDB_returnsHostWhenFound() throws Exception {
        when(mockZk.getChildren(eq("/semoss_root/app"), eq(false)))
                .thenReturn(Arrays.asList("myEngine@host1.example.com"));
        String host = client.getHostForDB("myEngine");
        assertEquals("host1.example.com", host);
    }

    @Test
    void testGetHostForDB_returnsNullWhenNotFound() throws Exception {
        when(mockZk.getChildren(eq("/semoss_root/app"), eq(false)))
                .thenReturn(Arrays.asList("otherEngine@host2"));
        String host = client.getHostForDB("myEngine");
        assertNull(host);
    }

    @Test
    void testGetHostForDB_matchesWithStartsWith() throws Exception {
        when(mockZk.getChildren(eq("/semoss_root/app"), eq(false)))
                .thenReturn(Arrays.asList("myApp@host3:8888"));
        String host = client.getHostForDB("myApp_InsightsRDBMS");
        assertEquals("host3:8888", host);
    }

    @Test
    void testGetHostForDB_returnsFirstMatch() throws Exception {
        when(mockZk.getChildren(eq("/semoss_root/app"), eq(false)))
                .thenReturn(Arrays.asList("appA@host1", "appB@host2", "appA@host3"));
        String host = client.getHostForDB("appA");
        assertEquals("host1", host);
    }

    @Test
    void testGetHostForDB_emptyChildrenList() throws Exception {
        when(mockZk.getChildren(eq("/semoss_root/app"), eq(false)))
                .thenReturn(Collections.emptyList());
        String host = client.getHostForDB("anyEngine");
        assertNull(host);
    }

    @Test
    void testGetHostForDB_usesCorrectPath() throws Exception {
        client.home = "/root";
        client.app = "/databases";
        when(mockZk.getChildren(eq("/root/databases"), eq(false)))
                .thenReturn(Arrays.asList("eng@host1"));
        client.getHostForDB("eng");
        verify(mockZk).getChildren(eq("/root/databases"), eq(false));
    }

    @Test
    void testTouchRoot_callsSetDataOnHomePath() throws Exception {
        when(mockZk.setData(anyString(), any(byte[].class), eq(-1))).thenReturn(new Stat());
        client.touchRoot();
        verify(mockZk).setData(eq("/semoss_root"), any(byte[].class), eq(-1));
    }

    @Test
    void testTouchRoot_withCustomHome() throws Exception {
        client.home = "/my_cluster";
        when(mockZk.setData(anyString(), any(byte[].class), eq(-1))).thenReturn(new Stat());
        client.touchRoot();
        verify(mockZk).setData(eq("/my_cluster"), any(byte[].class), eq(-1));
    }

    @Test
    void testTouchRoot_sendsDateString() throws Exception {
        when(mockZk.setData(anyString(), any(byte[].class), eq(-1))).thenReturn(new Stat());
        client.touchRoot();
        verify(mockZk).setData(eq("/semoss_root"), argThat(bytes -> {
            String dateStr = new String(bytes);
            return dateStr.matches("\\d{4}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2}");
        }), eq(-1));
    }

    @Test
    void testTouchRoot_usesVersionNegativeOne() throws Exception {
        when(mockZk.setData(anyString(), any(byte[].class), eq(-1))).thenReturn(new Stat());
        client.touchRoot();
        verify(mockZk).setData(anyString(), any(byte[].class), eq(-1));
    }

    @Test
    void testGetNodeData_returnsDataAsString() throws Exception {
        String path = "/semoss_root/app/myNode";
        when(mockZk.getData(eq(path), eq(true), any(Stat.class)))
                .thenReturn("node-data-value".getBytes("UTF-8"));
        String result = ZKClient.getNodeData(path, mockZk);
        assertEquals("node-data-value", result);
    }

    @Test
    void testGetNodeData_returnsNullOnException() throws Exception {
        String path = "/nonexistent";
        when(mockZk.getData(eq(path), eq(true), any(Stat.class)))
                .thenThrow(new org.apache.zookeeper.KeeperException.NoNodeException(path));
        String result = ZKClient.getNodeData(path, mockZk);
        assertNull(result);
    }

    @Test
    void testGetNodeData_handlesEmptyData() throws Exception {
        String path = "/empty";
        when(mockZk.getData(eq(path), eq(true), any(Stat.class)))
                .thenReturn("".getBytes("UTF-8"));
        String result = ZKClient.getNodeData(path, mockZk);
        assertEquals("", result);
    }

    @Test
    void testGetNodeData_handlesUTF8Data() throws Exception {
        String path = "/utf8node";
        String utf8Value = "Hello World 123";
        when(mockZk.getData(eq(path), eq(true), any(Stat.class)))
                .thenReturn(utf8Value.getBytes("UTF-8"));
        String result = ZKClient.getNodeData(path, mockZk);
        assertEquals(utf8Value, result);
    }

    @Test
    void testWatchEvent_addsListenerToMap() throws Exception {
        IZKListener listener = mock(IZKListener.class);
        String path = "/semoss_root/app";
        when(mockZk.getChildren(eq(path), eq(true), isNull()))
                .thenReturn(Collections.emptyList());
        client.watchEvent(path, EventType.NodeChildrenChanged, listener);
        WatchedEvent event = new WatchedEvent(EventType.NodeChildrenChanged, KeeperState.SyncConnected, path);
        client.process(event);
        verify(listener).process(eq(path), eq(mockZk));
    }

    @Test
    void testWatchEvent_multipleListenersForSameKey() throws Exception {
        IZKListener listener1 = mock(IZKListener.class);
        IZKListener listener2 = mock(IZKListener.class);
        String path = "/semoss_root/test";
        when(mockZk.getChildren(eq(path), eq(true), isNull()))
                .thenReturn(Collections.emptyList());
        client.watchEvent(path, EventType.NodeChildrenChanged, listener1);
        client.watchEvent(path, EventType.NodeChildrenChanged, listener2);
        WatchedEvent event = new WatchedEvent(EventType.NodeChildrenChanged, KeeperState.SyncConnected, path);
        client.process(event);
        verify(listener1).process(eq(path), eq(mockZk));
        verify(listener2).process(eq(path), eq(mockZk));
    }

    @Test
    void testWatchEvent_defaultWatchAgainIsTrue() throws Exception {
        IZKListener listener = mock(IZKListener.class);
        String path = "/semoss_root/watched";
        when(mockZk.getChildren(eq(path), eq(true), isNull()))
                .thenReturn(Collections.emptyList());
        client.watchEvent(path, EventType.NodeChildrenChanged, listener);
        WatchedEvent event = new WatchedEvent(EventType.NodeChildrenChanged, KeeperState.SyncConnected, path);
        client.process(event);
        verify(mockZk, times(2)).getChildren(eq(path), eq(true), isNull());
    }

    @Test
    void testWatchEvent_watchAgainFalse_doesNotReWatch() throws Exception {
        IZKListener listener = mock(IZKListener.class);
        String path = "/semoss_root/once";
        when(mockZk.getChildren(eq(path), eq(true), isNull()))
                .thenReturn(Collections.emptyList());
        client.watchEvent(path, EventType.NodeChildrenChanged, listener, false);
        WatchedEvent event = new WatchedEvent(EventType.NodeChildrenChanged, KeeperState.SyncConnected, path);
        client.process(event);
        verify(mockZk, times(1)).getChildren(eq(path), eq(true), isNull());
    }

    @Test
    void testWatchEvent_nodeDataChanged_callsWatchPathD() throws Exception {
        IZKListener listener = mock(IZKListener.class);
        String path = "/semoss_root/data";
        when(mockZk.getData(eq(path), eq(true), isNull()))
                .thenReturn("data".getBytes());
        client.watchEvent(path, EventType.NodeDataChanged, listener);
        verify(mockZk).getData(eq(path), eq(true), isNull());
    }

    @Test
    void testWatchEvent_nodeDeleted_callsWatchPathD() throws Exception {
        IZKListener listener = mock(IZKListener.class);
        String path = "/semoss_root/deleted";
        when(mockZk.getData(eq(path), eq(true), isNull()))
                .thenReturn("data".getBytes());
        client.watchEvent(path, EventType.NodeDeleted, listener);
        verify(mockZk).getData(eq(path), eq(true), isNull());
    }

    @Test
    void testProcess_invokesListenersForMatchingKey() throws Exception {
        IZKListener listener = mock(IZKListener.class);
        String path = "/semoss_root/app";
        when(mockZk.getChildren(eq(path), eq(true), isNull()))
                .thenReturn(Collections.emptyList());
        client.watchEvent(path, EventType.NodeChildrenChanged, listener);
        WatchedEvent event = new WatchedEvent(EventType.NodeChildrenChanged, KeeperState.SyncConnected, path);
        client.process(event);
        verify(listener).process(eq(path), eq(mockZk));
    }

    @Test
    void testProcess_doesNotInvokeListenersForDifferentEventType() throws Exception {
        IZKListener listener = mock(IZKListener.class);
        String path = "/semoss_root/app";
        when(mockZk.getChildren(eq(path), eq(true), isNull()))
                .thenReturn(Collections.emptyList());
        client.watchEvent(path, EventType.NodeChildrenChanged, listener);
        WatchedEvent event = new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, path);
        client.process(event);
        verify(listener, never()).process(anyString(), any(ZooKeeper.class));
    }

    @Test
    void testProcess_doesNotInvokeListenersForDifferentPath() throws Exception {
        IZKListener listener = mock(IZKListener.class);
        String path = "/semoss_root/app";
        when(mockZk.getChildren(eq(path), eq(true), isNull()))
                .thenReturn(Collections.emptyList());
        client.watchEvent(path, EventType.NodeChildrenChanged, listener);
        WatchedEvent event = new WatchedEvent(EventType.NodeChildrenChanged, KeeperState.SyncConnected, "/other/path");
        client.process(event);
        verify(listener, never()).process(anyString(), any(ZooKeeper.class));
    }

    @Test
    void testProcess_nullPathDoesNotThrow() {
        WatchedEvent event = new WatchedEvent(EventType.None, KeeperState.SyncConnected, null);
        assertDoesNotThrow(() -> client.process(event));
    }

    @Test
    void testProcess_nodeDataChanged_invokesListenerAndReWatches() throws Exception {
        IZKListener listener = mock(IZKListener.class);
        String path = "/semoss_root/data";
        when(mockZk.getData(eq(path), eq(true), isNull()))
                .thenReturn("data".getBytes());
        client.watchEvent(path, EventType.NodeDataChanged, listener, true);
        WatchedEvent event = new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, path);
        client.process(event);
        verify(listener).process(eq(path), eq(mockZk));
        verify(mockZk, times(2)).getData(eq(path), eq(true), isNull());
    }

    @Test
    void testProcess_nodeDeleted_invokesListenerAndReWatches() throws Exception {
        IZKListener listener = mock(IZKListener.class);
        String path = "/semoss_root/deleted";
        when(mockZk.getData(eq(path), eq(true), isNull()))
                .thenReturn("data".getBytes());
        client.watchEvent(path, EventType.NodeDeleted, listener, true);
        WatchedEvent event = new WatchedEvent(EventType.NodeDeleted, KeeperState.SyncConnected, path);
        client.process(event);
        verify(listener).process(eq(path), eq(mockZk));
        verify(mockZk, times(2)).getData(eq(path), eq(true), isNull());
    }

    @Test
    void testGetChildren_returnsChildList() throws Exception {
        List<String> expected = Arrays.asList("child1", "child2", "child3");
        when(mockZk.getChildren(eq("/semoss_root"), eq(true))).thenReturn(expected);
        List<String> result = client.getChildren("/semoss_root", true);
        assertEquals(expected, result);
    }

    @Test
    void testGetChildren_emptyList() throws Exception {
        when(mockZk.getChildren(eq("/empty"), eq(false))).thenReturn(Collections.emptyList());
        List<String> result = client.getChildren("/empty", false);
        assertTrue(result.isEmpty());
    }

    @Test
    void testWatchPath_callsGetChildren() throws Exception {
        when(mockZk.getChildren(eq("/test"), eq(true), isNull()))
                .thenReturn(Collections.emptyList());
        client.watchPath("/test");
        verify(mockZk).getChildren(eq("/test"), eq(true), isNull());
    }

    @Test
    void testWatchPathD_callsGetData() throws Exception {
        when(mockZk.getData(eq("/test"), eq(true), isNull()))
                .thenReturn("data".getBytes());
        client.watchPathD("/test");
        verify(mockZk).getData(eq("/test"), eq(true), isNull());
    }

    @Test
    void testPublishThenDeleteDB() throws Exception {
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL)))
                .thenReturn("/semoss_root/app/testEngine");
        client.publishDB("testEngine");
        client.deleteDB("testEngine");
        verify(mockZk).create(eq("/semoss_root/app/testEngine"), any(byte[].class),
                eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL));
        verify(mockZk).delete(eq("/semoss_root/app/testEngine"), eq(-1));
    }

    @Test
    void testPublishThenDeleteContainer() throws Exception {
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL)))
                .thenReturn("/semoss_root/container/host:8080");
        when(mockZk.setData(anyString(), any(byte[].class), eq(-1))).thenReturn(new Stat());
        client.publishContainer("host:8080");
        client.deleteContainer("host:8080");
        verify(mockZk).create(eq("/semoss_root/container/host:8080"), any(byte[].class),
                eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL));
        verify(mockZk).delete(eq("/semoss_root/container/host:8080"), eq(-1));
    }
    @Test
    void testGetVersion_setsVersion5() throws Exception {
        Stat s = new Stat(); s.setVersion(5);
        when(mockZk.exists(eq("/tn"), eq(true))).thenReturn(s);
        client.getVersion("/tn");
        assertEquals(5, client.version);
    }

    @Test
    void testGetVersion_setsVersion42() throws Exception {
        Stat s = new Stat(); s.setVersion(42);
        when(mockZk.exists(eq("/p42"), eq(true))).thenReturn(s);
        client.getVersion("/p42");
        assertEquals(42, client.version);
    }

    @Test
    void testGetVersion_setsVersion0() throws Exception {
        Stat s = new Stat(); s.setVersion(0);
        when(mockZk.exists(eq("/p0"), eq(true))).thenReturn(s);
        client.getVersion("/p0");
        assertEquals(0, client.version);
    }

    @Test
    void testWatchPath_verifiesArgs() throws Exception {
        when(mockZk.getChildren(eq("/wp"), eq(true), isNull()))
                .thenReturn(java.util.Arrays.asList("c1"));
        client.watchPath("/wp");
        verify(mockZk).getChildren(eq("/wp"), eq(true), isNull());
    }

    @Test
    void testWatchPathD_verifiesArgs() throws Exception {
        when(mockZk.getData(eq("/dp"), eq(true), isNull()))
                .thenReturn("data".getBytes());
        client.watchPathD("/dp");
        verify(mockZk).getData(eq("/dp"), eq(true), isNull());
    }

    @Test
    void testProcess_nullPath_childChanged_noThrow() {
        WatchedEvent ev = new WatchedEvent(EventType.NodeChildrenChanged, KeeperState.SyncConnected, null);
        assertDoesNotThrow(() -> client.process(ev));
    }

    @Test
    void testProcess_noneEvent_null_noThrow() {
        WatchedEvent ev = new WatchedEvent(EventType.None, KeeperState.Disconnected, null);
        assertDoesNotThrow(() -> client.process(ev));
    }

    @Test
    void testProcess_repeatTrue_reWatches() throws Exception {
        IZKListener li = mock(IZKListener.class);
        String p = "/semoss_root/rep";
        when(mockZk.getChildren(eq(p), eq(true), isNull())).thenReturn(Collections.emptyList());
        client.watchEvent(p, EventType.NodeChildrenChanged, li, true);
        client.process(new WatchedEvent(EventType.NodeChildrenChanged, KeeperState.SyncConnected, p));
        verify(li, times(1)).process(eq(p), eq(mockZk));
        verify(mockZk, times(2)).getChildren(eq(p), eq(true), isNull());
    }

    @Test
    void testProcess_repeatFalse_noReWatch() throws Exception {
        IZKListener li = mock(IZKListener.class);
        String p = "/semoss_root/once2";
        when(mockZk.getData(eq(p), eq(true), isNull())).thenReturn("d".getBytes());
        client.watchEvent(p, EventType.NodeDataChanged, li, false);
        client.process(new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, p));
        verify(li, times(1)).process(eq(p), eq(mockZk));
        verify(mockZk, times(1)).getData(eq(p), eq(true), isNull());
    }

    @Test
    void testProcess_noListeners_noThrow() {
        WatchedEvent ev = new WatchedEvent(EventType.NodeChildrenChanged, KeeperState.SyncConnected, "/unreg");
        assertDoesNotThrow(() -> client.process(ev));
    }

    @Test
    void testWatch4Data_calls() throws Exception {
        when(mockZk.getData(eq("/w4d"), eq(true), any(Stat.class))).thenReturn("x".getBytes());
        client.watch4Data("/w4d");
        verify(mockZk).getData(eq("/w4d"), eq(true), any(Stat.class));
    }

    @Test
    void testWatch4Children_calls() throws Exception {
        when(mockZk.getChildren(eq("/w4c"), eq(true))).thenReturn(Arrays.asList("c"));
        client.watch4Children("/w4c");
        verify(mockZk).getChildren(eq("/w4c"), eq(true));
    }

    @Test
    void testWatchSchedulerNode_true() throws Exception {
        when(mockZk.exists(eq("/sn"), eq(true))).thenReturn(new Stat());
        assertTrue(client.watchSchedulerNode("/sn", true));
    }

    @Test
    void testWatchSchedulerNode_false() throws Exception {
        when(mockZk.exists(eq("/sn2"), eq(false))).thenReturn(null);
        assertFalse(client.watchSchedulerNode("/sn2", false));
    }

    @Test
    void testCreateSchedulerNode_existing() throws Exception {
        when(mockZk.exists(eq("/ex"), eq(true))).thenReturn(new Stat());
        assertEquals("/ex", client.createSchedulerNode("/ex", true, true));
    }

    @Test void testRemoveWatch_noOp() { assertDoesNotThrow(() -> client.removeWatch("/p", EventType.NodeChildrenChanged)); }
    @Test void testDefaultVersionIsZero() { assertEquals(0, client.version); }
    @Test void testDefaultConnectedIsFalse() { assertFalse(client.connected); }
    @Test void testDefaultSemossHome() { assertEquals("/opt/semosshome/", ZKClient.semossHome); }
}
