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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NGINXDomainListenerUnitTests {

    private NGINXDomainListener listener;
    private String originalSemossHome;

    @BeforeEach
    void setUp() throws Exception {
        listener = new NGINXDomainListener();
        originalSemossHome = getSemossHome();
    }

    @AfterEach
    void tearDown() throws Exception {
        setSemossHome(originalSemossHome);
    }

    @Test
    void testConstructorCreatesInstance() {
        assertNotNull(listener);
    }

    @Test
    void testInstanceImplementsIZKListener() {
        assertInstanceOf(IZKListener.class, listener);
    }

    @Test
    void testSemossHomeConstantValue() {
        assertEquals("sem", NGINXDomainListener.SEMOSS_HOME);
    }

    @Test
    void testChildPathsStartsEmpty() throws Exception {
        Field f = NGINXDomainListener.class.getDeclaredField("childPaths");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<String> childPaths = (List<String>) f.get(listener);
        assertNotNull(childPaths);
        assertTrue(childPaths.isEmpty());
    }

    @Test
    void testChildPathsIsArrayList() throws Exception {
        Field f = NGINXDomainListener.class.getDeclaredField("childPaths");
        f.setAccessible(true);
        assertInstanceOf(ArrayList.class, f.get(listener));
    }

    @Test
    void testDomains2WatchStartsNull() throws Exception {
        Field f = NGINXDomainListener.class.getDeclaredField("domains2Watch");
        f.setAccessible(true);
        assertNull(f.get(listener));
    }

    @Test
    void testAppListenerStartsNull() throws Exception {
        Field f = NGINXDomainListener.class.getDeclaredField("appListener");
        f.setAccessible(true);
        assertNull(f.get(listener));
    }

    @Test
    void testGetNodeDataReturnsUtf8String() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        byte[] testData = "hello-world".getBytes("UTF-8");
        when(mockZk.getData(eq("/test/path"), eq(true), any(Stat.class))).thenReturn(testData);
        assertEquals("hello-world", NGINXDomainListener.getNodeData("/test/path", mockZk));
    }

    @Test
    void testGetNodeDataReturnsNullOnKeeperException() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getData(eq("/test/path"), eq(true), any(Stat.class)))
                .thenThrow(new KeeperException.NoNodeException("/test/path"));
        assertNull(NGINXDomainListener.getNodeData("/test/path", mockZk));
    }

    @Test
    void testGetNodeDataReturnsNullOnInterruptedException() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getData(eq("/test/path"), eq(true), any(Stat.class)))
                .thenThrow(new InterruptedException("test"));
        assertNull(NGINXDomainListener.getNodeData("/test/path", mockZk));
    }

    @Test
    void testGetNodeDataWithEmptyBytes() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getData(eq("/empty"), eq(true), any(Stat.class))).thenReturn(new byte[0]);
        assertEquals("", NGINXDomainListener.getNodeData("/empty", mockZk));
    }

    @Test
    void testGetNodeDataReturnsNullOnConnectionLoss() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getData(eq("/conn"), eq(true), any(Stat.class)))
                .thenThrow(new KeeperException.ConnectionLossException());
        assertNull(NGINXDomainListener.getNodeData("/conn", mockZk));
    }

    @Test
    void testGetNodeDataReturnsNullOnSessionExpired() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getData(eq("/expired"), eq(true), any(Stat.class)))
                .thenThrow(new KeeperException.SessionExpiredException());
        assertNull(NGINXDomainListener.getNodeData("/expired", mockZk));
    }

    @Test
    void testGetNodeDataPassesTrueForWatch() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getData(anyString(), eq(true), any(Stat.class))).thenReturn("data".getBytes("UTF-8"));
        NGINXDomainListener.getNodeData("/verify-watch", mockZk);
        verify(mockZk).getData(eq("/verify-watch"), eq(true), any(Stat.class));
    }

    @Test
    void testBackupCopiesNginxConf(@TempDir Path tempDir) throws Exception {
        Path confDir = tempDir.resolve("nginx").resolve("conf");
        Files.createDirectories(confDir);
        Files.writeString(confDir.resolve("nginx.conf"), "server { listen 80; }");
        setSemossHome(tempDir.toString().replace("\\", "/") + "/");
        listener.backup();
        Path workingConf = confDir.resolve("nginx-working.conf");
        assertTrue(Files.exists(workingConf), "nginx-working.conf should be created");
        assertEquals("server { listen 80; }", Files.readString(workingConf));
    }

    @Test
    void testBackupWhenNginxConfDoesNotExist(@TempDir Path tempDir) throws Exception {
        Path confDir = tempDir.resolve("nginx").resolve("conf");
        Files.createDirectories(confDir);
        setSemossHome(tempDir.toString().replace("\\", "/") + "/");
        assertDoesNotThrow(() -> listener.backup());
        assertFalse(Files.exists(confDir.resolve("nginx-working.conf")));
    }

    @Test
    void testBackupDeletesExistingWorkingConf(@TempDir Path tempDir) throws Exception {
        Path confDir = tempDir.resolve("nginx").resolve("conf");
        Files.createDirectories(confDir);
        Files.writeString(confDir.resolve("nginx.conf"), "updated config");
        Files.writeString(confDir.resolve("nginx-working.conf"), "old config");
        setSemossHome(tempDir.toString().replace("\\", "/") + "/");
        listener.backup();
        assertEquals("updated config", Files.readString(confDir.resolve("nginx-working.conf")));
    }

    @Test
    void testBackupPreservesOriginalFile(@TempDir Path tempDir) throws Exception {
        Path confDir = tempDir.resolve("nginx").resolve("conf");
        Files.createDirectories(confDir);
        Files.writeString(confDir.resolve("nginx.conf"), "original content");
        setSemossHome(tempDir.toString().replace("\\", "/") + "/");
        listener.backup();
        assertEquals("original content", Files.readString(confDir.resolve("nginx.conf")));
    }

    @Test
    void testBackupWithEmptyConfigFile(@TempDir Path tempDir) throws Exception {
        Path confDir = tempDir.resolve("nginx").resolve("conf");
        Files.createDirectories(confDir);
        Files.writeString(confDir.resolve("nginx.conf"), "");
        setSemossHome(tempDir.toString().replace("\\", "/") + "/");
        listener.backup();
        assertTrue(Files.exists(confDir.resolve("nginx-working.conf")));
        assertEquals("", Files.readString(confDir.resolve("nginx-working.conf")));
    }

    @Test
    void testBackupWhenConfDirDoesNotExist(@TempDir Path tempDir) throws Exception {
        setSemossHome(tempDir.toString().replace("\\", "/") + "/");
        assertDoesNotThrow(() -> listener.backup());
    }

    @Test
    void testReloadNginxDoesNotThrow() {
        assertDoesNotThrow(() -> listener.reloadNginx());
    }

    @Test
    void testReloadNginxCanBeCalledMultipleTimes() {
        assertDoesNotThrow(() -> { listener.reloadNginx(); listener.reloadNginx(); listener.reloadNginx(); });
    }

    @Test
    void testGetListenerReturnsNGINXAppListener() throws Exception {
        Method m = NGINXDomainListener.class.getDeclaredMethod("getListener");
        m.setAccessible(true);
        assertNotNull(m.invoke(listener));
        assertInstanceOf(NGINXAppListener.class, m.invoke(listener));
    }

    @Test
    void testGetListenerReturnsSameInstanceOnRepeatedCalls() throws Exception {
        Method m = NGINXDomainListener.class.getDeclaredMethod("getListener");
        m.setAccessible(true);
        assertSame(m.invoke(listener), m.invoke(listener));
    }

    @Test
    void testGetListenerSetsAppListenerField() throws Exception {
        Field f = NGINXDomainListener.class.getDeclaredField("appListener");
        f.setAccessible(true);
        assertNull(f.get(listener));
        Method m = NGINXDomainListener.class.getDeclaredMethod("getListener");
        m.setAccessible(true);
        m.invoke(listener);
        assertNotNull(f.get(listener));
    }

    @Test
    void testGetListenerReturnsIZKListener() throws Exception {
        Method m = NGINXDomainListener.class.getDeclaredMethod("getListener");
        m.setAccessible(true);
        assertInstanceOf(IZKListener.class, m.invoke(listener));
    }

    @Test
    void testProcessCallsZkGetChildren() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getChildren(eq("/root"), isNull())).thenReturn(new ArrayList<>());
        NGINXDomainListener testListener = new NGINXDomainListener() {
            @Override protected void watchDomains(List<String> paths) { }
            @Override public void genNginx(java.util.Map map) { }
        };
        testListener.process("/root", mockZk);
        verify(mockZk).getChildren(eq("/root"), isNull());
    }

    @Test
    void testProcessHandlesKeeperException() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getChildren(eq("/root"), isNull()))
                .thenThrow(new KeeperException.NoNodeException("/root"));
        assertDoesNotThrow(() -> listener.process("/root", mockZk));
    }

    @Test
    void testProcessHandlesInterruptedException() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getChildren(eq("/root"), isNull()))
                .thenThrow(new InterruptedException("test"));
        assertDoesNotThrow(() -> listener.process("/root", mockZk));
    }

    @Test
    void testRegenConfigSetsDomains2Watch() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        List<String> domains = Arrays.asList("domain1", "domain2");
        when(mockZk.getChildren(eq("/root"), isNull())).thenReturn(domains);
        when(mockZk.getChildren(eq("/root/domain1"), isNull())).thenReturn(new ArrayList<>());
        when(mockZk.getChildren(eq("/root/domain2"), isNull())).thenReturn(new ArrayList<>());
        NGINXDomainListener testListener = new NGINXDomainListener() {
            @Override protected void watchDomains(List<String> paths) { }
            @Override public void genNginx(java.util.Map map) { }
        };
        testListener.regenConfig("/root", mockZk);
        Field f = NGINXDomainListener.class.getDeclaredField("domains2Watch");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) f.get(testListener);
        assertEquals(domains, result);
    }

    @Test
    void testRegenConfigSkipsAppDomain() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getChildren(eq("/root"), isNull())).thenReturn(Arrays.asList("app", "realdomain"));
        when(mockZk.getChildren(eq("/root/app"), isNull())).thenReturn(Arrays.asList("child1"));
        when(mockZk.getChildren(eq("/root/realdomain"), isNull())).thenReturn(Arrays.asList("child2"));
        when(mockZk.getData(eq("/root/app/child1"), eq(true), any(Stat.class))).thenReturn("url1".getBytes("UTF-8"));
        when(mockZk.getData(eq("/root/realdomain/child2"), eq(true), any(Stat.class))).thenReturn("url2".getBytes("UTF-8"));
        final java.util.Map<?, ?>[] capturedMap = new java.util.Map[1];
        NGINXDomainListener testListener = new NGINXDomainListener() {
            @Override protected void watchDomains(List<String> paths) { }
            @Override public void genNginx(java.util.Map map) { capturedMap[0] = map; }
        };
        testListener.regenConfig("/root", mockZk);
        assertFalse(capturedMap[0].containsKey("app"));
        assertTrue(capturedMap[0].containsKey("realdomain"));
    }

    @Test
    void testRegenConfigBuildsChildPaths() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getChildren(eq("/root"), isNull())).thenReturn(Arrays.asList("domain1"));
        when(mockZk.getChildren(eq("/root/domain1"), isNull())).thenReturn(Arrays.asList("server1", "server2"));
        when(mockZk.getData(eq("/root/domain1/server1"), eq(true), any(Stat.class))).thenReturn("http://s1:8080".getBytes("UTF-8"));
        when(mockZk.getData(eq("/root/domain1/server2"), eq(true), any(Stat.class))).thenReturn("http://s2:8080".getBytes("UTF-8"));
        final List<String> watchedPaths = new ArrayList<>();
        NGINXDomainListener testListener = new NGINXDomainListener() {
            @Override protected void watchDomains(List<String> paths) { watchedPaths.addAll(paths); }
            @Override public void genNginx(java.util.Map map) { }
        };
        testListener.regenConfig("/root", mockZk);
        assertEquals(2, watchedPaths.size());
        assertTrue(watchedPaths.contains("/root/domain1/server1"));
        assertTrue(watchedPaths.contains("/root/domain1/server2"));
    }

    @Test
    void testRegenConfigWithEmptyDomainList() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getChildren(eq("/root"), isNull())).thenReturn(new ArrayList<>());
        final java.util.Map<?, ?>[] capturedMap = new java.util.Map[1];
        NGINXDomainListener testListener = new NGINXDomainListener() {
            @Override protected void watchDomains(List<String> paths) { }
            @Override public void genNginx(java.util.Map map) { capturedMap[0] = map; }
        };
        testListener.regenConfig("/root", mockZk);
        assertTrue(capturedMap[0].isEmpty());
    }

    @Test
    void testRegenConfigSkipsDomainWithEmptyChildren() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getChildren(eq("/root"), isNull())).thenReturn(Arrays.asList("emptydomain"));
        when(mockZk.getChildren(eq("/root/emptydomain"), isNull())).thenReturn(new ArrayList<>());
        final java.util.Map<?, ?>[] capturedMap = new java.util.Map[1];
        NGINXDomainListener testListener = new NGINXDomainListener() {
            @Override protected void watchDomains(List<String> paths) { }
            @Override public void genNginx(java.util.Map map) { capturedMap[0] = map; }
        };
        testListener.regenConfig("/root", mockZk);
        assertFalse(capturedMap[0].containsKey("emptydomain"));
    }

    @Test
    void testSemossHomeIsAccessible() throws Exception {
        assertNotNull(getSemossHome());
    }

    @Test
    void testSemossHomeCanBeSet(@TempDir Path tempDir) throws Exception {
        String newHome = tempDir.toString().replace("\\", "/") + "/";
        setSemossHome(newHome);
        assertEquals(newHome, getSemossHome());
    }

    private String getSemossHome() throws Exception {
        Field f = NGINXDomainListener.class.getDeclaredField("semossHome");
        f.setAccessible(true);
        return (String) f.get(null);
    }

    private void setSemossHome(String value) throws Exception {
        Field f = NGINXDomainListener.class.getDeclaredField("semossHome");
        f.setAccessible(true);
        f.set(null, value);
    }
}
