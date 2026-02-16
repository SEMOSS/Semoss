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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NGINXAppListenerUnitTests {

    private NGINXAppListener listener;
    private String originalSemossHome;

    @BeforeEach
    void setUp() throws Exception {
        listener = new NGINXAppListener();
        originalSemossHome = getAppSemossHome();
    }

    @AfterEach
    void tearDown() throws Exception {
        setAppSemossHome(originalSemossHome);
    }

    @Test
    void testConstructorCreatesInstance() {
        assertNotNull(listener);
    }

    @Test
    void testIsInstanceOfNGINXDomainListener() {
        assertInstanceOf(NGINXDomainListener.class, listener);
    }

    @Test
    void testImplementsIZKListener() {
        assertInstanceOf(IZKListener.class, listener);
    }

    @Test
    void testSuperclassIsNGINXDomainListener() {
        assertEquals(NGINXDomainListener.class, NGINXAppListener.class.getSuperclass());
    }

    @Test
    void testSemossHomeConstantValue() {
        assertEquals("sem", NGINXAppListener.SEMOSS_HOME);
    }

    @Test
    void testSemossHomeConstantSameAsParent() {
        assertEquals(NGINXDomainListener.SEMOSS_HOME, NGINXAppListener.SEMOSS_HOME);
    }

    @Test
    void testGetNodeDataReturnsUtf8String() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getData(eq("/app/node"), eq(true), any(Stat.class))).thenReturn("test-data".getBytes("UTF-8"));
        assertEquals("test-data", NGINXAppListener.getNodeData("/app/node", mockZk));
    }

    @Test
    void testGetNodeDataReturnsNullOnKeeperException() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getData(eq("/app/node"), eq(true), any(Stat.class)))
                .thenThrow(new KeeperException.NoNodeException("/app/node"));
        assertNull(NGINXAppListener.getNodeData("/app/node", mockZk));
    }

    @Test
    void testGetNodeDataReturnsNullOnInterruptedException() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getData(eq("/app/node"), eq(true), any(Stat.class)))
                .thenThrow(new InterruptedException("test"));
        assertNull(NGINXAppListener.getNodeData("/app/node", mockZk));
    }

    @Test
    void testGetNodeDataWithEmptyString() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getData(eq("/empty"), eq(true), any(Stat.class))).thenReturn(new byte[0]);
        assertEquals("", NGINXAppListener.getNodeData("/empty", mockZk));
    }

    @Test
    void testGetNodeDataReturnsNullOnConnectionLoss() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getData(eq("/conn"), eq(true), any(Stat.class)))
                .thenThrow(new KeeperException.ConnectionLossException());
        assertNull(NGINXAppListener.getNodeData("/conn", mockZk));
    }

    @Test
    void testGetNodeDataPassesTrueForWatch() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getData(anyString(), eq(true), any(Stat.class))).thenReturn("x".getBytes("UTF-8"));
        NGINXAppListener.getNodeData("/verify", mockZk);
        verify(mockZk).getData(eq("/verify"), eq(true), any(Stat.class));
    }

    @Test
    void testWatchDomainsDoesNotThrow() {
        assertDoesNotThrow(() -> listener.watchDomains("/some/path"));
    }

    @Test
    void testWatchDomainsWithNullPath() {
        assertDoesNotThrow(() -> listener.watchDomains((String) null));
    }

    @Test
    void testWatchDomainsWithEmptyPath() {
        assertDoesNotThrow(() -> listener.watchDomains(""));
    }

    @Test
    void testWatchDomainsCanBeCalledMultipleTimes() {
        assertDoesNotThrow(() -> { listener.watchDomains("/p1"); listener.watchDomains("/p2"); });
    }

    @Test
    void testReloadNginxDoesNotThrow() {
        assertDoesNotThrow(() -> listener.reloadNginx());
    }

    @Test
    void testReloadNginxCanBeCalledMultipleTimes() {
        assertDoesNotThrow(() -> { listener.reloadNginx(); listener.reloadNginx(); });
    }

    @Test
    void testBackupCopiesNginxConf(@TempDir Path tempDir) throws Exception {
        Path confDir = tempDir.resolve("nginx").resolve("conf");
        Files.createDirectories(confDir);
        Files.writeString(confDir.resolve("nginx.conf"), "upstream { server localhost:8080; }");
        setAppSemossHome(tempDir.toString().replace("\\", "/") + "/");
        listener.backup();
        Path workingConf = confDir.resolve("nginx-working.conf");
        assertTrue(Files.exists(workingConf));
        assertEquals("upstream { server localhost:8080; }", Files.readString(workingConf));
    }

    @Test
    void testBackupWhenSourceDoesNotExist(@TempDir Path tempDir) throws Exception {
        Path confDir = tempDir.resolve("nginx").resolve("conf");
        Files.createDirectories(confDir);
        setAppSemossHome(tempDir.toString().replace("\\", "/") + "/");
        assertDoesNotThrow(() -> listener.backup());
    }

    @Test
    void testBackupDeletesExistingWorkingConf(@TempDir Path tempDir) throws Exception {
        Path confDir = tempDir.resolve("nginx").resolve("conf");
        Files.createDirectories(confDir);
        Files.writeString(confDir.resolve("nginx.conf"), "new content");
        Files.writeString(confDir.resolve("nginx-working.conf"), "old content");
        setAppSemossHome(tempDir.toString().replace("\\", "/") + "/");
        listener.backup();
        assertEquals("new content", Files.readString(confDir.resolve("nginx-working.conf")));
    }

    @Test
    void testBackupPreservesOriginalFile(@TempDir Path tempDir) throws Exception {
        Path confDir = tempDir.resolve("nginx").resolve("conf");
        Files.createDirectories(confDir);
        Files.writeString(confDir.resolve("nginx.conf"), "keep me");
        setAppSemossHome(tempDir.toString().replace("\\", "/") + "/");
        listener.backup();
        assertEquals("keep me", Files.readString(confDir.resolve("nginx.conf")));
    }

    @Test
    void testBackupWithEmptyConfigFile(@TempDir Path tempDir) throws Exception {
        Path confDir = tempDir.resolve("nginx").resolve("conf");
        Files.createDirectories(confDir);
        Files.writeString(confDir.resolve("nginx.conf"), "");
        setAppSemossHome(tempDir.toString().replace("\\", "/") + "/");
        listener.backup();
        assertEquals("", Files.readString(confDir.resolve("nginx-working.conf")));
    }

    @Test
    void testBackupWhenConfDirDoesNotExist(@TempDir Path tempDir) throws Exception {
        setAppSemossHome(tempDir.toString().replace("\\", "/") + "/");
        assertDoesNotThrow(() -> listener.backup());
    }

    @Test
    void testProcessCallsZkGetChildren() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getChildren(eq("/root/domain"), isNull())).thenReturn(new ArrayList<>());
        listener.process("/root/domain/app/child", mockZk);
        verify(mockZk).getChildren(eq("/root/domain"), isNull());
    }

    @Test
    void testProcessHandlesKeeperException() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getChildren(anyString(), isNull()))
                .thenThrow(new KeeperException.NoNodeException("/test"));
        assertDoesNotThrow(() -> listener.process("/root/domain/app/child", mockZk));
    }

    @Test
    void testProcessHandlesInterruptedException() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getChildren(anyString(), isNull()))
                .thenThrow(new InterruptedException("test"));
        assertDoesNotThrow(() -> listener.process("/root/domain/app/child", mockZk));
    }

    @Test
    void testRegenConfigNavigatesUpTwoLevels() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getChildren(eq("/root/domain"), isNull())).thenReturn(new ArrayList<>());
        NGINXAppListener testListener = new NGINXAppListener() {
            @Override protected void watchDomains(String path) { }
            @Override public void genNginx(java.util.Map map) { }
        };
        testListener.regenConfig("/root/domain/app/child", mockZk);
        verify(mockZk).getChildren(eq("/root/domain"), isNull());
    }

    @Test
    void testRegenConfigNavigatesUpTwoLevelsFromDeepPath() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getChildren(eq("/a/b/c"), isNull())).thenReturn(new ArrayList<>());
        NGINXAppListener testListener = new NGINXAppListener() {
            @Override protected void watchDomains(String path) { }
            @Override public void genNginx(java.util.Map map) { }
        };
        testListener.regenConfig("/a/b/c/d/e", mockZk);
        verify(mockZk).getChildren(eq("/a/b/c"), isNull());
    }

    @Test
    void testRegenConfigSetsDomains2Watch() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        List<String> domains = Arrays.asList("domain1", "domain2");
        when(mockZk.getChildren(eq("/root/base"), isNull())).thenReturn(domains);
        when(mockZk.getChildren(eq("/root/base/domain1"), isNull())).thenReturn(new ArrayList<>());
        when(mockZk.getChildren(eq("/root/base/domain2"), isNull())).thenReturn(new ArrayList<>());
        NGINXAppListener testListener = new NGINXAppListener() {
            @Override protected void watchDomains(String path) { }
            @Override public void genNginx(java.util.Map map) { }
        };
        testListener.regenConfig("/root/base/seg1/seg2", mockZk);
        Field f = NGINXDomainListener.class.getDeclaredField("domains2Watch");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) f.get(testListener);
        assertEquals(domains, result);
    }

    @Test
    void testRegenConfigSkipsAppDomain() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getChildren(eq("/root/base"), isNull())).thenReturn(Arrays.asList("app", "realdomain"));
        when(mockZk.getChildren(eq("/root/base/app"), isNull())).thenReturn(Arrays.asList("c1"));
        when(mockZk.getChildren(eq("/root/base/realdomain"), isNull())).thenReturn(Arrays.asList("c2"));
        when(mockZk.getData(eq("/root/base/app/c1"), eq(true), any(Stat.class))).thenReturn("u1".getBytes("UTF-8"));
        when(mockZk.getData(eq("/root/base/realdomain/c2"), eq(true), any(Stat.class))).thenReturn("u2".getBytes("UTF-8"));
        final Map<?, ?>[] capturedMap = new Map[1];
        NGINXAppListener testListener = new NGINXAppListener() {
            @Override protected void watchDomains(String path) { }
            @Override public void genNginx(java.util.Map map) { capturedMap[0] = map; }
        };
        testListener.regenConfig("/root/base/x/y", mockZk);
        assertFalse(capturedMap[0].containsKey("app"));
        assertTrue(capturedMap[0].containsKey("realdomain"));
    }

    @Test
    void testRegenConfigPassesOriginalPathToWatchDomains() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getChildren(eq("/root/base"), isNull())).thenReturn(new ArrayList<>());
        final String[] capturedPath = new String[1];
        NGINXAppListener testListener = new NGINXAppListener() {
            @Override protected void watchDomains(String path) { capturedPath[0] = path; }
            @Override public void genNginx(java.util.Map map) { }
        };
        testListener.regenConfig("/root/base/x/y", mockZk);
        assertEquals("/root/base/x/y", capturedPath[0]);
    }

    @Test
    void testRegenConfigWithEmptyDomainList() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getChildren(eq("/root/base"), isNull())).thenReturn(new ArrayList<>());
        final Map<?, ?>[] capturedMap = new Map[1];
        NGINXAppListener testListener = new NGINXAppListener() {
            @Override protected void watchDomains(String path) { }
            @Override public void genNginx(java.util.Map map) { capturedMap[0] = map; }
        };
        testListener.regenConfig("/root/base/x/y", mockZk);
        assertTrue(capturedMap[0].isEmpty());
    }

    @Test
    void testSemossHomeFieldIsAccessible() throws Exception {
        assertNotNull(getAppSemossHome());
    }

    @Test
    void testSemossHomeCanBeSetViaReflection(@TempDir Path tempDir) throws Exception {
        String newHome = tempDir.toString().replace("\\", "/") + "/";
        setAppSemossHome(newHome);
        assertEquals(newHome, getAppSemossHome());
    }

    private String getAppSemossHome() throws Exception {
        Field f = NGINXAppListener.class.getDeclaredField("semossHome");
        f.setAccessible(true);
        return (String) f.get(null);
    }

    private void setAppSemossHome(String value) throws Exception {
        Field f = NGINXAppListener.class.getDeclaredField("semossHome");
        f.setAccessible(true);
        f.set(null, value);
    }
}
