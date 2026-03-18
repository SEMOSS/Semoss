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
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.engine.api.RemoteModelStateEnum;

class RemoteClientServerZKUnitTests {

    private RemoteClientServerZK proxy;
    private CuratorFramework mockClient;

    @BeforeEach
    void setUp() throws Exception {
        Constructor<RemoteClientServerZK> ctor =
                RemoteClientServerZK.class.getDeclaredConstructor();
        ctor.setAccessible(true);
        proxy = ctor.newInstance();

        mockClient = mock(CuratorFramework.class);
        setField("client", mockClient);
    }

    @AfterEach
    void tearDown() throws Exception {
        Field instanceField = RemoteClientServerZK.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        instanceField.set(null, null);
    }

    // ========== Reflection helpers ==========

    private void setField(String fieldName, Object value) throws Exception {
        Field f = RemoteClientServerZK.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(proxy, value);
    }

    @SuppressWarnings("unchecked")
    private ConcurrentMap<String, RemoteModelStateEnum> getModelStates() throws Exception {
        Field f = RemoteClientServerZK.class.getDeclaredField("modelStates");
        f.setAccessible(true);
        return (ConcurrentMap<String, RemoteModelStateEnum>) f.get(proxy);
    }

    @SuppressWarnings("unchecked")
    private ConcurrentMap<String, String> getModelClusterIps() throws Exception {
        Field f = RemoteClientServerZK.class.getDeclaredField("modelClusterIps");
        f.setAccessible(true);
        return (ConcurrentMap<String, String>) f.get(proxy);
    }

    @SuppressWarnings("unchecked")
    private ConcurrentMap<String, String> getModelNames() throws Exception {
        Field f = RemoteClientServerZK.class.getDeclaredField("modelNames");
        f.setAccessible(true);
        return (ConcurrentMap<String, String>) f.get(proxy);
    }

    private ScheduledExecutorService getScheduler() throws Exception {
        Field f = RemoteClientServerZK.class.getDeclaredField("scheduler");
        f.setAccessible(true);
        return (ScheduledExecutorService) f.get(proxy);
    }

    private Object invokePrivate(String methodName, Class<?>[] paramTypes, Object... args) throws Exception {
        Method m = RemoteClientServerZK.class.getDeclaredMethod(methodName, paramTypes);
        m.setAccessible(true);
        return m.invoke(proxy, args);
    }

    // ========== Curator mock helpers ==========

    private void mockCheckExists(String path, boolean exists) throws Exception {
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(mockClient.checkExists()).thenReturn(existsBuilder);
        when(existsBuilder.forPath(path)).thenReturn(exists ? new Stat() : null);
    }

    private void mockCheckExistsAny(boolean exists) throws Exception {
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(mockClient.checkExists()).thenReturn(existsBuilder);
        when(existsBuilder.forPath(anyString())).thenReturn(exists ? new Stat() : null);
    }

    private void mockGetData(String path, byte[] data) throws Exception {
        GetDataBuilder getDataBuilder = mock(GetDataBuilder.class);
        when(mockClient.getData()).thenReturn(getDataBuilder);
        when(getDataBuilder.forPath(path)).thenReturn(data);
    }

    private void mockGetChildren(String path, List<String> children) throws Exception {
        GetChildrenBuilder getChildrenBuilder = mock(GetChildrenBuilder.class);
        when(mockClient.getChildren()).thenReturn(getChildrenBuilder);
        when(getChildrenBuilder.forPath(path)).thenReturn(children);
    }

    private byte[] modelJsonBytes(String ip, String modelName) {
        JSONObject json = new JSONObject();
        json.put("ip", ip);
        json.put("model_name", modelName);
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    // ========== Tests: getModelState ==========

    @Test
    void testGetModelState_activeModel() throws Exception {
        getModelStates().put("model-1", RemoteModelStateEnum.ACTIVE);
        assertEquals(RemoteModelStateEnum.ACTIVE, proxy.getModelState("model-1"));
    }

    @Test
    void testGetModelState_warmingModel() throws Exception {
        getModelStates().put("model-2", RemoteModelStateEnum.WARMING);
        assertEquals(RemoteModelStateEnum.WARMING, proxy.getModelState("model-2"));
    }

    @Test
    void testGetModelState_coldModel() throws Exception {
        getModelStates().put("model-3", RemoteModelStateEnum.COLD);
        assertEquals(RemoteModelStateEnum.COLD, proxy.getModelState("model-3"));
    }

    @Test
    void testGetModelState_failedModel() throws Exception {
        getModelStates().put("model-4", RemoteModelStateEnum.FAILED);
        assertEquals(RemoteModelStateEnum.FAILED, proxy.getModelState("model-4"));
    }

    @Test
    void testGetModelState_unknownModel_defaultsToCold() {
        assertEquals(RemoteModelStateEnum.COLD, proxy.getModelState("nonexistent"));
    }

    @Test
    void testGetModelState_emptyModelId() {
        assertEquals(RemoteModelStateEnum.COLD, proxy.getModelState(""));
    }

    @Test
    void testGetModelState_nullModelId_throwsNPE() {
        assertThrows(NullPointerException.class, () -> proxy.getModelState(null));
    }

    // ========== Tests: getModelName ==========

    @Test
    void testGetModelName_known() throws Exception {
        getModelNames().put("model-1", "GPT-4");
        assertEquals("GPT-4", proxy.getModelName("model-1"));
    }

    @Test
    void testGetModelName_unknown() {
        assertNull(proxy.getModelName("nonexistent"));
    }

    @Test
    void testGetModelName_emptyString() throws Exception {
        getModelNames().put("model-1", "");
        assertEquals("", proxy.getModelName("model-1"));
    }

    @Test
    void testGetModelName_multipleModels() throws Exception {
        getModelNames().put("m1", "Model-A");
        getModelNames().put("m2", "Model-B");
        getModelNames().put("m3", "Model-C");
        assertEquals("Model-A", proxy.getModelName("m1"));
        assertEquals("Model-B", proxy.getModelName("m2"));
        assertEquals("Model-C", proxy.getModelName("m3"));
    }

    // ========== Tests: getModelScalerIp ==========

    @Test
    void testGetModelScalerIp_set() throws Exception {
        setField("modelScalerIp", "10.0.0.100");
        assertEquals("10.0.0.100", proxy.getModelScalerIp());
    }

    @Test
    void testGetModelScalerIp_null() {
        assertNull(proxy.getModelScalerIp());
    }

    // ========== Tests: getModelClusterIp ==========

    @Test
    void testGetModelClusterIp_success() throws Exception {
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(mockClient.checkExists()).thenReturn(existsBuilder);
        when(existsBuilder.forPath("/models/active/m1")).thenReturn(new Stat());

        GetDataBuilder getDataBuilder = mock(GetDataBuilder.class);
        when(mockClient.getData()).thenReturn(getDataBuilder);
        when(getDataBuilder.forPath("/models/active/m1")).thenReturn(modelJsonBytes("10.0.0.5", "TestModel"));

        String ip = proxy.getModelClusterIp("m1");
        assertEquals("10.0.0.5", ip);
        assertEquals("10.0.0.5", getModelClusterIps().get("m1"));
        assertEquals("TestModel", getModelNames().get("m1"));
    }

    @Test
    void testGetModelClusterIp_pathDoesNotExist() throws Exception {
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(mockClient.checkExists()).thenReturn(existsBuilder);
        when(existsBuilder.forPath("/models/active/m1")).thenReturn(null);

        assertNull(proxy.getModelClusterIp("m1"));
    }

    @Test
    void testGetModelClusterIp_noData() throws Exception {
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(mockClient.checkExists()).thenReturn(existsBuilder);
        when(existsBuilder.forPath("/models/active/m1")).thenReturn(new Stat());

        GetDataBuilder getDataBuilder = mock(GetDataBuilder.class);
        when(mockClient.getData()).thenReturn(getDataBuilder);
        when(getDataBuilder.forPath("/models/active/m1")).thenReturn(null);

        assertNull(proxy.getModelClusterIp("m1"));
    }

    @Test
    void testGetModelClusterIp_emptyData() throws Exception {
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(mockClient.checkExists()).thenReturn(existsBuilder);
        when(existsBuilder.forPath("/models/active/m1")).thenReturn(new Stat());

        GetDataBuilder getDataBuilder = mock(GetDataBuilder.class);
        when(mockClient.getData()).thenReturn(getDataBuilder);
        when(getDataBuilder.forPath("/models/active/m1")).thenReturn(new byte[0]);

        assertNull(proxy.getModelClusterIp("m1"));
    }

    @Test
    void testGetModelClusterIp_exception() throws Exception {
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(mockClient.checkExists()).thenReturn(existsBuilder);
        when(existsBuilder.forPath(anyString())).thenThrow(new RuntimeException("ZK error"));

        assertNull(proxy.getModelClusterIp("m1"));
    }

    @Test
    void testGetModelClusterIp_updatesCache() throws Exception {
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(mockClient.checkExists()).thenReturn(existsBuilder);
        when(existsBuilder.forPath("/models/active/m1")).thenReturn(new Stat());

        GetDataBuilder getDataBuilder = mock(GetDataBuilder.class);
        when(mockClient.getData()).thenReturn(getDataBuilder);
        when(getDataBuilder.forPath("/models/active/m1")).thenReturn(modelJsonBytes("10.0.0.99", "CachedModel"));

        proxy.getModelClusterIp("m1");
        assertEquals("10.0.0.99", getModelClusterIps().get("m1"));
        assertEquals("CachedModel", getModelNames().get("m1"));
    }

    // ========== Tests: isModelWarming ==========

    @Test
    void testIsModelWarming_true() throws Exception {
        mockCheckExists("/models/warming/m1", true);
        assertTrue(proxy.isModelWarming("m1"));
    }

    @Test
    void testIsModelWarming_false() throws Exception {
        mockCheckExists("/models/warming/m1", false);
        assertFalse(proxy.isModelWarming("m1"));
    }

    @Test
    void testIsModelWarming_exception() throws Exception {
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(mockClient.checkExists()).thenReturn(existsBuilder);
        when(existsBuilder.forPath(anyString())).thenThrow(new RuntimeException("ZK error"));
        assertFalse(proxy.isModelWarming("m1"));
    }

    // ========== Tests: isModelActive ==========

    @Test
    void testIsModelActive_true() throws Exception {
        mockCheckExists("/models/active/m1", true);
        assertTrue(proxy.isModelActive("m1"));
    }

    @Test
    void testIsModelActive_false() throws Exception {
        mockCheckExists("/models/active/m1", false);
        assertFalse(proxy.isModelActive("m1"));
    }

    @Test
    void testIsModelActive_exception() throws Exception {
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(mockClient.checkExists()).thenReturn(existsBuilder);
        when(existsBuilder.forPath(anyString())).thenThrow(new RuntimeException("ZK error"));
        assertFalse(proxy.isModelActive("m1"));
    }

    // ========== Tests: waitForModelActive ==========

    @Test
    void testWaitForModelActive_alreadyActive() throws Exception {
        mockCheckExistsAny(true);
        assertTrue(proxy.waitForModelActive("m1", 5000));
    }

    @Test
    void testWaitForModelActive_timeout() throws Exception {
        mockCheckExistsAny(false);
        assertFalse(proxy.waitForModelActive("m1", 100));
    }

    // ========== Tests: waitForState ==========

    @Test
    void testWaitForState_alreadyInDesiredState() throws Exception {
        getModelStates().put("m1", RemoteModelStateEnum.ACTIVE);
        assertTrue(proxy.waitForState("m1", RemoteModelStateEnum.ACTIVE, 5000));
    }

    @Test
    void testWaitForState_defaultsCold() {
        assertTrue(proxy.waitForState("nonexistent", RemoteModelStateEnum.COLD, 5000));
    }

    @Test
    void testWaitForState_warmingModel_timeout() throws Exception {
        getModelStates().put("m1", RemoteModelStateEnum.WARMING);
        assertFalse(proxy.waitForState("m1", RemoteModelStateEnum.ACTIVE, 100));
    }

    @Test
    void testWaitForState_failedModel_earlyExit() throws Exception {
        getModelStates().put("m1", RemoteModelStateEnum.FAILED);
        assertFalse(proxy.waitForState("m1", RemoteModelStateEnum.ACTIVE, 5000));
    }

    @Test
    void testWaitForState_waitingForWarming() throws Exception {
        getModelStates().put("m1", RemoteModelStateEnum.WARMING);
        assertTrue(proxy.waitForState("m1", RemoteModelStateEnum.WARMING, 5000));
    }

    // ========== Tests: getActiveModels ==========

    @Test
    void testGetActiveModels_withCachedNames() throws Exception {
        mockGetChildren("/models/active", List.of("m1", "m2"));

        getModelNames().put("m1", "Model-A");
        getModelNames().put("m2", "Model-B");
        getModelStates().put("m1", RemoteModelStateEnum.ACTIVE);
        getModelStates().put("m2", RemoteModelStateEnum.ACTIVE);

        List<RemoteModelInfo> result = proxy.getActiveModels();
        assertEquals(2, result.size());
        assertEquals("m1", result.get(0).getId());
        assertEquals("Model-A", result.get(0).getName());
        assertEquals(RemoteModelStateEnum.ACTIVE, result.get(0).getState());
        assertEquals("m2", result.get(1).getId());
        assertEquals("Model-B", result.get(1).getName());
    }

    @Test
    void testGetActiveModels_empty() throws Exception {
        mockGetChildren("/models/active", new ArrayList<>());

        List<RemoteModelInfo> result = proxy.getActiveModels();
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetActiveModels_unknownNameFallsBackToZK() throws Exception {
        GetChildrenBuilder childrenBuilder = mock(GetChildrenBuilder.class);
        when(mockClient.getChildren()).thenReturn(childrenBuilder);
        when(childrenBuilder.forPath("/models/active")).thenReturn(List.of("m1"));

        GetDataBuilder getDataBuilder = mock(GetDataBuilder.class);
        when(mockClient.getData()).thenReturn(getDataBuilder);
        when(getDataBuilder.forPath("/models/active/m1")).thenReturn(modelJsonBytes("10.0.0.1", "FetchedModel"));

        List<RemoteModelInfo> result = proxy.getActiveModels();
        assertEquals(1, result.size());
        assertEquals("m1", result.get(0).getId());
        assertEquals("FetchedModel", result.get(0).getName());
        assertEquals("FetchedModel", getModelNames().get("m1"));
    }

    @Test
    void testGetActiveModels_unknownNameAndNullData() throws Exception {
        GetChildrenBuilder childrenBuilder = mock(GetChildrenBuilder.class);
        when(mockClient.getChildren()).thenReturn(childrenBuilder);
        when(childrenBuilder.forPath("/models/active")).thenReturn(List.of("m1"));

        GetDataBuilder getDataBuilder = mock(GetDataBuilder.class);
        when(mockClient.getData()).thenReturn(getDataBuilder);
        when(getDataBuilder.forPath("/models/active/m1")).thenReturn(null);

        List<RemoteModelInfo> result = proxy.getActiveModels();
        // Model with null data is not added
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetActiveModels_unknownNameAndEmptyData() throws Exception {
        GetChildrenBuilder childrenBuilder = mock(GetChildrenBuilder.class);
        when(mockClient.getChildren()).thenReturn(childrenBuilder);
        when(childrenBuilder.forPath("/models/active")).thenReturn(List.of("m1"));

        GetDataBuilder getDataBuilder = mock(GetDataBuilder.class);
        when(mockClient.getData()).thenReturn(getDataBuilder);
        when(getDataBuilder.forPath("/models/active/m1")).thenReturn(new byte[0]);

        List<RemoteModelInfo> result = proxy.getActiveModels();
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetActiveModels_exception() throws Exception {
        GetChildrenBuilder childrenBuilder = mock(GetChildrenBuilder.class);
        when(mockClient.getChildren()).thenReturn(childrenBuilder);
        when(childrenBuilder.forPath(anyString())).thenThrow(new RuntimeException("ZK error"));

        List<RemoteModelInfo> result = proxy.getActiveModels();
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetActiveModels_stateDefaultsToCold() throws Exception {
        mockGetChildren("/models/active", List.of("m1"));
        getModelNames().put("m1", "Model-A");
        // no state in modelStates

        List<RemoteModelInfo> result = proxy.getActiveModels();
        assertEquals(1, result.size());
        assertEquals(RemoteModelStateEnum.COLD, result.get(0).getState());
    }

    // ========== Tests: getWarmingModels ==========

    @Test
    void testGetWarmingModels_withCachedNames() throws Exception {
        mockGetChildren("/models/warming", List.of("w1", "w2"));

        getModelNames().put("w1", "Warming-A");
        getModelStates().put("w1", RemoteModelStateEnum.WARMING);
        getModelStates().put("w2", RemoteModelStateEnum.WARMING);

        List<RemoteModelInfo> result = proxy.getWarmingModels();
        assertEquals(2, result.size());
        assertEquals("w1", result.get(0).getId());
        assertEquals("Warming-A", result.get(0).getName());
        assertEquals(RemoteModelStateEnum.WARMING, result.get(0).getState());
        assertEquals("w2", result.get(1).getId());
        assertEquals("Warming...", result.get(1).getName());
    }

    @Test
    void testGetWarmingModels_empty() throws Exception {
        mockGetChildren("/models/warming", new ArrayList<>());

        List<RemoteModelInfo> result = proxy.getWarmingModels();
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetWarmingModels_unknownNameDefaultsToWarmingText() throws Exception {
        mockGetChildren("/models/warming", List.of("w1"));

        List<RemoteModelInfo> result = proxy.getWarmingModels();
        assertEquals(1, result.size());
        assertEquals("Warming...", result.get(0).getName());
    }

    @Test
    void testGetWarmingModels_stateDefaultsToWarming() throws Exception {
        mockGetChildren("/models/warming", List.of("w1"));
        // No state in modelStates

        List<RemoteModelInfo> result = proxy.getWarmingModels();
        assertEquals(1, result.size());
        assertEquals(RemoteModelStateEnum.WARMING, result.get(0).getState());
    }

    @Test
    void testGetWarmingModels_exception() throws Exception {
        GetChildrenBuilder childrenBuilder = mock(GetChildrenBuilder.class);
        when(mockClient.getChildren()).thenReturn(childrenBuilder);
        when(childrenBuilder.forPath(anyString())).thenThrow(new RuntimeException("ZK error"));

        List<RemoteModelInfo> result = proxy.getWarmingModels();
        assertTrue(result.isEmpty());
    }

    // ========== Tests: close ==========

    @Test
    void testClose_closesAllResources() throws Exception {
        CuratorCache warmingCache = mock(CuratorCache.class);
        CuratorCache activeCache = mock(CuratorCache.class);
        setField("warmingCache", warmingCache);
        setField("activeCache", activeCache);

        proxy.close();

        verify(warmingCache).close();
        verify(activeCache).close();
        verify(mockClient).close();
    }

    @Test
    void testClose_nullCaches() throws Exception {
        setField("warmingCache", null);
        setField("activeCache", null);

        assertDoesNotThrow(() -> proxy.close());
        verify(mockClient).close();
    }

    @Test
    void testClose_nullClient() throws Exception {
        setField("warmingCache", null);
        setField("activeCache", null);
        setField("client", null);

        assertDoesNotThrow(() -> proxy.close());
    }

    @Test
    void testClose_exceptionInCacheClose() throws Exception {
        CuratorCache warmingCache = mock(CuratorCache.class);
        doThrow(new RuntimeException("close error")).when(warmingCache).close();
        setField("warmingCache", warmingCache);
        setField("activeCache", null);

        // Should not propagate the exception
        assertDoesNotThrow(() -> proxy.close());
    }

    // ========== Tests: getModelIdFromPath (private) ==========

    @Test
    void testGetModelIdFromPath_standardPath() throws Exception {
        String result = (String) invokePrivate("getModelIdFromPath",
                new Class[]{String.class}, "/models/active/model-123");
        assertEquals("model-123", result);
    }

    @Test
    void testGetModelIdFromPath_warmingPath() throws Exception {
        String result = (String) invokePrivate("getModelIdFromPath",
                new Class[]{String.class}, "/models/warming/abc-def");
        assertEquals("abc-def", result);
    }

    @Test
    void testGetModelIdFromPath_singleSegment() throws Exception {
        String result = (String) invokePrivate("getModelIdFromPath",
                new Class[]{String.class}, "model-123");
        assertEquals("model-123", result);
    }

    @Test
    void testGetModelIdFromPath_trailingSlash() throws Exception {
        String result = (String) invokePrivate("getModelIdFromPath",
                new Class[]{String.class}, "/models/active/model-1/");
        // lastIndexOf('/') + 1 gives empty string when trailing slash
        assertEquals("", result);
    }

    @Test
    void testGetModelIdFromPath_deepPath() throws Exception {
        String result = (String) invokePrivate("getModelIdFromPath",
                new Class[]{String.class}, "/a/b/c/d/model-99");
        assertEquals("model-99", result);
    }

    // ========== Tests: getModelPath (private) ==========

    @Test
    void testGetModelPath_returnsModelId() throws Exception {
        String result = (String) invokePrivate("getModelPath",
                new Class[]{String.class}, "model-123");
        assertEquals("model-123", result);
    }

    @Test
    void testGetModelPath_emptyString() throws Exception {
        String result = (String) invokePrivate("getModelPath",
                new Class[]{String.class}, "");
        assertEquals("", result);
    }

    // ========== Tests: concurrent map state ==========

    @Test
    void testConcurrentMapState_multipleModels() throws Exception {
        getModelStates().put("m1", RemoteModelStateEnum.ACTIVE);
        getModelStates().put("m2", RemoteModelStateEnum.WARMING);
        getModelStates().put("m3", RemoteModelStateEnum.COLD);
        getModelStates().put("m4", RemoteModelStateEnum.FAILED);

        getModelClusterIps().put("m1", "10.0.0.1");
        getModelClusterIps().put("m2", "10.0.0.2");

        getModelNames().put("m1", "Model-A");
        getModelNames().put("m2", "Model-B");
        getModelNames().put("m3", "Model-C");

        assertEquals(RemoteModelStateEnum.ACTIVE, proxy.getModelState("m1"));
        assertEquals(RemoteModelStateEnum.WARMING, proxy.getModelState("m2"));
        assertEquals(RemoteModelStateEnum.COLD, proxy.getModelState("m3"));
        assertEquals(RemoteModelStateEnum.FAILED, proxy.getModelState("m4"));

        assertEquals("Model-A", proxy.getModelName("m1"));
        assertEquals("Model-B", proxy.getModelName("m2"));
        assertEquals("Model-C", proxy.getModelName("m3"));
        assertNull(proxy.getModelName("m4"));
    }

    @Test
    void testModelStateOverwrite() throws Exception {
        getModelStates().put("m1", RemoteModelStateEnum.WARMING);
        assertEquals(RemoteModelStateEnum.WARMING, proxy.getModelState("m1"));

        getModelStates().put("m1", RemoteModelStateEnum.ACTIVE);
        assertEquals(RemoteModelStateEnum.ACTIVE, proxy.getModelState("m1"));

        getModelStates().put("m1", RemoteModelStateEnum.COLD);
        assertEquals(RemoteModelStateEnum.COLD, proxy.getModelState("m1"));
    }

    // ========== Tests: refreshModelStates (private) ==========

    @Test
    void testRefreshModelStates_updatesStates() throws Exception {
        GetChildrenBuilder childrenBuilder = mock(GetChildrenBuilder.class);
        when(mockClient.getChildren()).thenReturn(childrenBuilder);
        when(childrenBuilder.forPath("/models/active")).thenReturn(List.of("m1"));
        when(childrenBuilder.forPath("/models/warming")).thenReturn(List.of("m2"));

        // m2 needs an existing non-ACTIVE state for the warming check
        getModelStates().put("m2", RemoteModelStateEnum.COLD);

        invokePrivate("refreshModelStates", new Class[]{});
        assertEquals(RemoteModelStateEnum.ACTIVE, getModelStates().get("m1"));
        assertEquals(RemoteModelStateEnum.WARMING, getModelStates().get("m2"));
    }

    @Test
    void testRefreshModelStates_staleEntriesBecomeCold() throws Exception {
        getModelStates().put("stale-model", RemoteModelStateEnum.ACTIVE);

        GetChildrenBuilder childrenBuilder = mock(GetChildrenBuilder.class);
        when(mockClient.getChildren()).thenReturn(childrenBuilder);
        when(childrenBuilder.forPath("/models/active")).thenReturn(new ArrayList<>());
        when(childrenBuilder.forPath("/models/warming")).thenReturn(new ArrayList<>());

        invokePrivate("refreshModelStates", new Class[]{});
        assertEquals(RemoteModelStateEnum.COLD, getModelStates().get("stale-model"));
    }

    @Test
    void testRefreshModelStates_activeOverridesWarming() throws Exception {
        GetChildrenBuilder childrenBuilder = mock(GetChildrenBuilder.class);
        when(mockClient.getChildren()).thenReturn(childrenBuilder);
        when(childrenBuilder.forPath("/models/active")).thenReturn(List.of("m1"));
        when(childrenBuilder.forPath("/models/warming")).thenReturn(List.of("m1"));

        invokePrivate("refreshModelStates", new Class[]{});
        assertEquals(RemoteModelStateEnum.ACTIVE, getModelStates().get("m1"));
    }

    // ========== Tests: loadInitialState (private) ==========

    @Test
    void testLoadInitialState_loadsActiveAndWarming() throws Exception {
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(mockClient.checkExists()).thenReturn(existsBuilder);
        when(existsBuilder.forPath("/services/kube-model-deployer")).thenReturn(new Stat());

        GetDataBuilder getDataBuilder = mock(GetDataBuilder.class);
        when(mockClient.getData()).thenReturn(getDataBuilder);
        when(getDataBuilder.forPath("/services/kube-model-deployer")).thenReturn("10.0.0.100".getBytes(StandardCharsets.UTF_8));
        when(getDataBuilder.forPath("/models/active/a1")).thenReturn(modelJsonBytes("10.0.0.10", "ActiveModel"));

        GetChildrenBuilder childrenBuilder = mock(GetChildrenBuilder.class);
        when(mockClient.getChildren()).thenReturn(childrenBuilder);
        when(childrenBuilder.forPath("/models/active")).thenReturn(List.of("a1"));
        when(childrenBuilder.forPath("/models/warming")).thenReturn(List.of("w1"));

        invokePrivate("loadInitialState", new Class[]{});

        assertEquals("10.0.0.100", proxy.getModelScalerIp());
        assertEquals(RemoteModelStateEnum.ACTIVE, getModelStates().get("a1"));
        assertEquals("10.0.0.10", getModelClusterIps().get("a1"));
        assertEquals("ActiveModel", getModelNames().get("a1"));
        assertEquals(RemoteModelStateEnum.WARMING, getModelStates().get("w1"));
    }

    @Test
    void testLoadInitialState_noScalerPath() throws Exception {
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(mockClient.checkExists()).thenReturn(existsBuilder);
        when(existsBuilder.forPath("/services/kube-model-deployer")).thenReturn(null);

        GetChildrenBuilder childrenBuilder = mock(GetChildrenBuilder.class);
        when(mockClient.getChildren()).thenReturn(childrenBuilder);
        when(childrenBuilder.forPath("/models/active")).thenReturn(new ArrayList<>());
        when(childrenBuilder.forPath("/models/warming")).thenReturn(new ArrayList<>());

        invokePrivate("loadInitialState", new Class[]{});
        assertNull(proxy.getModelScalerIp());
    }

    @Test
    void testLoadInitialState_scalerPathEmptyData() throws Exception {
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(mockClient.checkExists()).thenReturn(existsBuilder);
        when(existsBuilder.forPath("/services/kube-model-deployer")).thenReturn(new Stat());

        GetDataBuilder getDataBuilder = mock(GetDataBuilder.class);
        when(mockClient.getData()).thenReturn(getDataBuilder);
        when(getDataBuilder.forPath("/services/kube-model-deployer")).thenReturn(new byte[0]);

        GetChildrenBuilder childrenBuilder = mock(GetChildrenBuilder.class);
        when(mockClient.getChildren()).thenReturn(childrenBuilder);
        when(childrenBuilder.forPath("/models/active")).thenReturn(new ArrayList<>());
        when(childrenBuilder.forPath("/models/warming")).thenReturn(new ArrayList<>());

        invokePrivate("loadInitialState", new Class[]{});
        assertNull(proxy.getModelScalerIp());
    }

    @Test
    void testLoadInitialState_exception_doesNotThrow() throws Exception {
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(mockClient.checkExists()).thenReturn(existsBuilder);
        when(existsBuilder.forPath(anyString())).thenThrow(new RuntimeException("ZK error"));

        assertDoesNotThrow(() -> invokePrivate("loadInitialState", new Class[]{}));
    }

    @Test
    void testLoadInitialState_activeModelWithNullData() throws Exception {
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(mockClient.checkExists()).thenReturn(existsBuilder);
        when(existsBuilder.forPath("/services/kube-model-deployer")).thenReturn(null);

        GetDataBuilder getDataBuilder = mock(GetDataBuilder.class);
        when(mockClient.getData()).thenReturn(getDataBuilder);
        when(getDataBuilder.forPath("/models/active/a1")).thenReturn(null);

        GetChildrenBuilder childrenBuilder = mock(GetChildrenBuilder.class);
        when(mockClient.getChildren()).thenReturn(childrenBuilder);
        when(childrenBuilder.forPath("/models/active")).thenReturn(List.of("a1"));
        when(childrenBuilder.forPath("/models/warming")).thenReturn(new ArrayList<>());

        invokePrivate("loadInitialState", new Class[]{});
        // a1 should not be in modelStates since data was null
        assertNull(getModelStates().get("a1"));
    }

    // ========== Tests: devPortForwarding field ==========

    @Test
    void testDevPortForwarding_defaultFalse() throws Exception {
        Field f = RemoteClientServerZK.class.getDeclaredField("devPortForwarding");
        f.setAccessible(true);
        assertEquals(false, f.get(proxy));
    }

    // ========== Tests: zkServer field ==========

    @Test
    void testZkServer_default() throws Exception {
        Field f = RemoteClientServerZK.class.getDeclaredField("zkServer");
        f.setAccessible(true);
        assertEquals("localhost:2181", f.get(proxy));
    }
}