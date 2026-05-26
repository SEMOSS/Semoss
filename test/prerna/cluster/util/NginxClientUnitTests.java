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

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NginxClientUnitTests {

    private NginxClient client;
    private ZooKeeper mockZk;

    @BeforeEach
    void setUp() throws Exception {
        Constructor<NginxClient> ctor = NginxClient.class.getDeclaredConstructor();
        ctor.setAccessible(true);
        client = ctor.newInstance();
        mockZk = mock(ZooKeeper.class);
        Field zkField = NginxClient.class.getDeclaredField("zk");
        zkField.setAccessible(true);
        zkField.set(client, mockZk);
    }

    @Test
    void testConstants_haveExpectedValues() {
        assertEquals("zk", NginxClient.ZK_SERVER);
        assertEquals("host", NginxClient.HOST);
        assertEquals("to", NginxClient.TIMEOUT);
        assertEquals("bu", NginxClient.BOOTUSER);
        assertEquals("home", NginxClient.HOME);
        assertEquals("app", NginxClient.APP_HOME);
    }

    @Test
    void testDefaultFieldValues() {
        assertEquals("192.168.99.100:2181", client.zkServer);
        assertEquals("192.168.99.100:8888", client.host);
        assertEquals("generic", client.user);
        assertEquals("/semoss_root", client.home);
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
    void testGetPayload_usesActualHostAndUser() {
        String payload = client.getPayload();
        assertTrue(payload.contains("url=192.168.99.100:8888"));
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
        client.publishNode();
        verify(mockZk).create(eq("/semoss_root/semoss"), any(byte[].class),
                eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL_SEQUENTIAL));
    }

    @Test
    void testPublishNode_payloadMatchesGetPayload() throws Exception {
        String expectedPayload = client.getPayload();
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL_SEQUENTIAL)))
                .thenReturn("/semoss_root/semoss0000000001");
        client.publishNode();
        verify(mockZk).create(eq("/semoss_root/semoss"), eq(expectedPayload.getBytes()),
                eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL_SEQUENTIAL));
    }

    @Test
    void testPublishNode_withCustomHome() throws Exception {
        client.home = "/custom_root";
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL_SEQUENTIAL)))
                .thenReturn("/custom_root/semoss0000000001");
        client.publishNode();
        verify(mockZk).create(eq("/custom_root/semoss"), any(byte[].class),
                eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL_SEQUENTIAL));
    }

    @Test
    void testPublishNode_doesNotCallTouchRoot() throws Exception {
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL_SEQUENTIAL)))
                .thenReturn("/semoss_root/semoss0000000001");
        client.publishNode();
        verify(mockZk, never()).setData(anyString(), any(byte[].class), anyInt());
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
        verify(mockZk).create(eq("/semoss_root/app/engine1"), eq("192.168.99.100:8888".getBytes()),
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
    void testGetNodeData_returnsDataAsString() throws Exception {
        String path = "/semoss_root/app/myNode";
        when(mockZk.getData(eq(path), eq(true), any(Stat.class)))
                .thenReturn("node-data-value".getBytes("UTF-8"));
        String result = client.getNodeData(path);
        assertEquals("node-data-value", result);
    }

    @Test
    void testGetNodeData_returnsNullOnKeeperException() throws Exception {
        String path = "/nonexistent";
        when(mockZk.getData(eq(path), eq(true), any(Stat.class)))
                .thenThrow(new org.apache.zookeeper.KeeperException.NoNodeException(path));
        String result = client.getNodeData(path);
        assertNull(result);
    }

    @Test
    void testGetNodeData_handlesEmptyData() throws Exception {
        String path = "/empty";
        when(mockZk.getData(eq(path), eq(true), any(Stat.class)))
                .thenReturn("".getBytes("UTF-8"));
        String result = client.getNodeData(path);
        assertEquals("", result);
    }

    @Test
    void testGetNodeData_handlesUTF8Data() throws Exception {
        String path = "/utf8node";
        String value = "Hello World 123 Test";
        when(mockZk.getData(eq(path), eq(true), any(Stat.class)))
                .thenReturn(value.getBytes("UTF-8"));
        String result = client.getNodeData(path);
        assertEquals(value, result);
    }

    @Test
    void testGetNodeData_callsGetDataWithWatchTrue() throws Exception {
        String path = "/test/path";
        when(mockZk.getData(eq(path), eq(true), any(Stat.class)))
                .thenReturn("data".getBytes("UTF-8"));
        client.getNodeData(path);
        verify(mockZk).getData(eq(path), eq(true), any(Stat.class));
    }

    @Test
    void testGetNodeData_returnsNullOnInterruptedException() throws Exception {
        String path = "/interrupted";
        when(mockZk.getData(eq(path), eq(true), any(Stat.class)))
                .thenThrow(new InterruptedException("test interrupt"));
        String result = client.getNodeData(path);
        assertNull(result);
    }

    @Test
    void testWatchForChildren_callsGetChildrenWithWatch() throws Exception {
        when(mockZk.getChildren(eq("/semoss_root/app"), eq(true), isNull()))
                .thenReturn(Collections.emptyList());
        client.watchForChildren();
        verify(mockZk).getChildren(eq("/semoss_root/app"), eq(true), isNull());
    }

    @Test
    void testWatchForChildren_withCustomHomeAndApp() throws Exception {
        client.home = "/root";
        client.app = "/databases";
        when(mockZk.getChildren(eq("/root/databases"), eq(true), isNull()))
                .thenReturn(Collections.emptyList());
        client.watchForChildren();
        verify(mockZk).getChildren(eq("/root/databases"), eq(true), isNull());
    }

    @Test
    void testWatchForChildren_returnsChildList() throws Exception {
        List<String> children = Arrays.asList("engine1", "engine2");
        when(mockZk.getChildren(eq("/semoss_root/app"), eq(true), isNull()))
                .thenReturn(children);
        client.watchForChildren();
        verify(mockZk).getChildren(eq("/semoss_root/app"), eq(true), isNull());
    }

    @Test
    void testWatchForChildren_handlesKeeperException() throws Exception {
        when(mockZk.getChildren(eq("/semoss_root/app"), eq(true), isNull()))
                .thenThrow(new org.apache.zookeeper.KeeperException.NoNodeException("/semoss_root/app"));
        assertDoesNotThrow(() -> client.watchForChildren());
    }

    @Test
    void testBackup_copiesNginxConfToWorkingConf(@TempDir Path tempDir) throws Exception {
        Path confDir = tempDir.resolve("nginx").resolve("conf");
        Files.createDirectories(confDir);
        Path nginxConf = confDir.resolve("nginx.conf");
        Files.writeString(nginxConf, "upstream backend { server 127.0.0.1:8080; }");
        client.semossHome = tempDir.toString() + File.separator;
        client.backup();
        Path workingConf = confDir.resolve("nginx-working.conf");
        assertTrue(Files.exists(workingConf), "nginx-working.conf should exist after backup");
        assertEquals("upstream backend { server 127.0.0.1:8080; }",
                Files.readString(workingConf));
    }

    @Test
    void testBackup_overwritesExistingWorkingConf(@TempDir Path tempDir) throws Exception {
        Path confDir = tempDir.resolve("nginx").resolve("conf");
        Files.createDirectories(confDir);
        Files.writeString(confDir.resolve("nginx.conf"), "new config content");
        Files.writeString(confDir.resolve("nginx-working.conf"), "old config content");
        client.semossHome = tempDir.toString() + File.separator;
        client.backup();
        String result = Files.readString(confDir.resolve("nginx-working.conf"));
        assertEquals("new config content", result);
    }

    @Test
    void testBackup_handlesNoExistingWorkingConf(@TempDir Path tempDir) throws Exception {
        Path confDir = tempDir.resolve("nginx").resolve("conf");
        Files.createDirectories(confDir);
        Files.writeString(confDir.resolve("nginx.conf"), "fresh config");
        client.semossHome = tempDir.toString() + File.separator;
        assertDoesNotThrow(() -> client.backup());
        assertTrue(Files.exists(confDir.resolve("nginx-working.conf")));
        assertEquals("fresh config", Files.readString(confDir.resolve("nginx-working.conf")));
    }

    @Test
    void testBackup_sourceDoesNotExist_doesNotThrow(@TempDir Path tempDir) throws Exception {
        Path confDir = tempDir.resolve("nginx").resolve("conf");
        Files.createDirectories(confDir);
        client.semossHome = tempDir.toString() + File.separator;
        assertDoesNotThrow(() -> client.backup());
    }

    @Test
    void testBackup_preservesOriginalFile(@TempDir Path tempDir) throws Exception {
        Path confDir = tempDir.resolve("nginx").resolve("conf");
        Files.createDirectories(confDir);
        String originalContent = "worker_processes auto;\nevents { worker_connections 1024; }";
        Files.writeString(confDir.resolve("nginx.conf"), originalContent);
        client.semossHome = tempDir.toString() + File.separator;
        client.backup();
        assertEquals(originalContent, Files.readString(confDir.resolve("nginx.conf")));
        assertEquals(originalContent, Files.readString(confDir.resolve("nginx-working.conf")));
    }

    @Test
    void testPublishNodeThenPublishDB() throws Exception {
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL_SEQUENTIAL)))
                .thenReturn("/semoss_root/semoss0000000001");
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL)))
                .thenReturn("/semoss_root/app/testDB");
        client.publishNode();
        client.publishDB("testDB");
        verify(mockZk).create(eq("/semoss_root/semoss"), any(byte[].class),
                eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL_SEQUENTIAL));
        verify(mockZk).create(eq("/semoss_root/app/testDB"), any(byte[].class),
                eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL));
    }
    @Test void testNodeCreated_doesNotThrow() { assertDoesNotThrow(() -> client.nodeCreated("p")); }
    @Test void testNodeCreated_null_doesNotThrow() { assertDoesNotThrow(() -> client.nodeCreated(null)); }
    @Test void testNodeCreated_empty_doesNotThrow() { assertDoesNotThrow(() -> client.nodeCreated("")); }
    @Test void testNodeDeleted_doesNotThrow() { assertDoesNotThrow(() -> client.nodeDeleted("p")); }
    @Test void testNodeDeleted_null_doesNotThrow() { assertDoesNotThrow(() -> client.nodeDeleted(null)); }
    @Test void testNodeDeleted_empty_doesNotThrow() { assertDoesNotThrow(() -> client.nodeDeleted("")); }
    @Test void testReloadNginx_doesNotThrow() { assertDoesNotThrow(() -> client.reloadNginx()); }

    @Test
    void testGetVersion_setsVersion3() throws Exception {
        Stat ms = new Stat(); ms.setVersion(3);
        when(mockZk.exists(eq("/tv"), eq(true))).thenReturn(ms);
        client.getVersion("/tv");
        java.lang.reflect.Field vf = NginxClient.class.getDeclaredField("version");
        vf.setAccessible(true);
        assertEquals(3, (int) vf.get(client));
    }

    @Test
    void testGetVersion_setsVersion17() throws Exception {
        Stat ms = new Stat(); ms.setVersion(17);
        when(mockZk.exists(eq("/a2"), eq(true))).thenReturn(ms);
        client.getVersion("/a2");
        java.lang.reflect.Field vf = NginxClient.class.getDeclaredField("version");
        vf.setAccessible(true);
        assertEquals(17, (int) vf.get(client));
    }

    @Test
    void testGetVersion_setsVersion0() throws Exception {
        Stat ms = new Stat(); ms.setVersion(0);
        when(mockZk.exists(eq("/z0"), eq(true))).thenReturn(ms);
        client.getVersion("/z0");
        java.lang.reflect.Field vf = NginxClient.class.getDeclaredField("version");
        vf.setAccessible(true);
        assertEquals(0, (int) vf.get(client));
    }

    @Test
    void testNodeChildChanged_emptyChildren() throws Exception {
        String path = "/semoss_root/app";
        when(mockZk.getChildren(eq(path), isNull())).thenReturn(java.util.Collections.emptyList());
        assertDoesNotThrow(() -> client.nodeChildChanged(path));
        verify(mockZk).getChildren(eq(path), isNull());
    }

    @Test
    void testProcess_nodeChildrenChanged_callsWatchForChildren() throws Exception {
        String path = "/semoss_root/app";
        when(mockZk.getChildren(eq(path), eq(true), isNull())).thenReturn(Collections.emptyList());
        when(mockZk.getChildren(eq(path), isNull())).thenReturn(Collections.emptyList());
        WatchedEvent ev = new WatchedEvent(EventType.NodeChildrenChanged, KeeperState.SyncConnected, path);
        assertDoesNotThrow(() -> client.process(ev));
        verify(mockZk, atLeastOnce()).getChildren(eq(path), eq(true), isNull());
    }

    @Test
    void testProcess_nullPath_doesNotThrow() throws Exception {
        WatchedEvent ev = new WatchedEvent(EventType.None, KeeperState.SyncConnected, null);
        assertDoesNotThrow(() -> client.process(ev));
    }

    @Test
    void testProcess_noneEvent_doesNotThrow() throws Exception {
        when(mockZk.getChildren(anyString(), eq(true), isNull())).thenReturn(Collections.emptyList());
        WatchedEvent ev = new WatchedEvent(EventType.None, KeeperState.Disconnected, "/some/path");
        assertDoesNotThrow(() -> client.process(ev));
    }

    @Test
    void testWatchForChildren_emptyAppPath() throws Exception {
        client.home = "/root"; client.app = "";
        when(mockZk.getChildren(eq("/root"), eq(true), isNull())).thenReturn(Collections.emptyList());
        client.watchForChildren();
        verify(mockZk).getChildren(eq("/root"), eq(true), isNull());
    }

    @Test
    void testWatchForChildren_interruptedException() throws Exception {
        when(mockZk.getChildren(eq("/semoss_root/app"), eq(true), isNull()))
                .thenThrow(new InterruptedException("test"));
        assertDoesNotThrow(() -> client.watchForChildren());
    }

    @Test void testDefaultVersionIsZero() throws Exception {
        java.lang.reflect.Field vf = NginxClient.class.getDeclaredField("version");
        vf.setAccessible(true); assertEquals(0, (int) vf.get(client));
    }
    @Test void testDefaultConnectedIsFalse() throws Exception {
        java.lang.reflect.Field cf = NginxClient.class.getDeclaredField("connected");
        cf.setAccessible(true); assertFalse((boolean) cf.get(client));
    }
    @Test void testDefaultSemossHome() { assertNotNull(client.semossHome); }

    @Test void testGetPayload_memoryPositive() {
        String pl = client.getPayload();
        int ms = pl.indexOf("memory=") + 7;
        int me = pl.indexOf("|", ms);
        assertTrue(Long.parseLong(pl.substring(ms, me)) > 0);
    }
    @Test void testGetPayload_endsWithPipe() { assertTrue(client.getPayload().endsWith("|")); }

    @Test void testPublishDB_emptyEngineId() throws Exception {
        when(mockZk.create(anyString(), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL)))
                .thenReturn("/semoss_root/app/");
        client.publishDB("");
        verify(mockZk).create(eq("/semoss_root/app/"), any(byte[].class), eq(ZooDefs.Ids.OPEN_ACL_UNSAFE), eq(CreateMode.EPHEMERAL));
    }

    @Test void testBackup_multipleBackups(@TempDir Path tempDir) throws Exception {
        Path confDir = tempDir.resolve("nginx").resolve("conf");
        Files.createDirectories(confDir);
        client.semossHome = tempDir.toString() + File.separator;
        Files.writeString(confDir.resolve("nginx.conf"), "v1");
        client.backup();
        assertEquals("v1", Files.readString(confDir.resolve("nginx-working.conf")));
        Files.writeString(confDir.resolve("nginx.conf"), "v2");
        client.backup();
        assertEquals("v2", Files.readString(confDir.resolve("nginx-working.conf")));
    }
}
