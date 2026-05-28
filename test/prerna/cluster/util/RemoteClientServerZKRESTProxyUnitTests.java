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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.engine.api.RemoteModelStateEnum;

class RemoteClientServerZKRESTProxyUnitTests {

    private RemoteClientServerZKRESTProxy proxy;

    @BeforeEach
    void setUp() throws Exception {
        Constructor<RemoteClientServerZKRESTProxy> ctor =
                RemoteClientServerZKRESTProxy.class.getDeclaredConstructor();
        ctor.setAccessible(true);
        proxy = ctor.newInstance();

        setField("zkRestProxyBaseUrl", "http://localhost:8080/");
    }

    @AfterEach
    void tearDown() throws Exception {
        Field instanceField = RemoteClientServerZKRESTProxy.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        instanceField.set(null, null);
    }

    // ========== Reflection helpers ==========

    private void setField(String fieldName, Object value) throws Exception {
        Field f = RemoteClientServerZKRESTProxy.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(proxy, value);
    }

    @SuppressWarnings("unchecked")
    private ConcurrentMap<String, RemoteModelStateEnum> getModelStates() throws Exception {
        Field f = RemoteClientServerZKRESTProxy.class.getDeclaredField("modelStates");
        f.setAccessible(true);
        return (ConcurrentMap<String, RemoteModelStateEnum>) f.get(proxy);
    }

    @SuppressWarnings("unchecked")
    private ConcurrentMap<String, String> getModelClusterIps() throws Exception {
        Field f = RemoteClientServerZKRESTProxy.class.getDeclaredField("modelClusterIps");
        f.setAccessible(true);
        return (ConcurrentMap<String, String>) f.get(proxy);
    }

    @SuppressWarnings("unchecked")
    private ConcurrentMap<String, String> getModelNames() throws Exception {
        Field f = RemoteClientServerZKRESTProxy.class.getDeclaredField("modelNames");
        f.setAccessible(true);
        return (ConcurrentMap<String, String>) f.get(proxy);
    }

    private ScheduledExecutorService getScheduler() throws Exception {
        Field f = RemoteClientServerZKRESTProxy.class.getDeclaredField("scheduler");
        f.setAccessible(true);
        return (ScheduledExecutorService) f.get(proxy);
    }

    // ========== HTTP mock helpers ==========

    private CloseableHttpResponse createMockResponse(int statusCode, String body) throws Exception {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);
        when(response.getStatusLine()).thenReturn(statusLine);

        if (body != null) {
            when(response.getEntity()).thenReturn(new StringEntity(body));
        } else {
            when(response.getEntity()).thenReturn(null);
        }

        return response;
    }

    private MockedStatic<HttpClients> setupHttpMock(CloseableHttpClient mockClient) {
        HttpClientBuilder mockBuilder = mock(HttpClientBuilder.class);
        when(mockBuilder.setDefaultRequestConfig(any())).thenReturn(mockBuilder);
        when(mockBuilder.build()).thenReturn(mockClient);

        MockedStatic<HttpClients> mockedHttpClients = mockStatic(HttpClients.class);
        mockedHttpClients.when(HttpClients::custom).thenReturn(mockBuilder);
        return mockedHttpClients;
    }

    // ========== Private method invocation helpers ==========

    private Object invokePrivate(String methodName, Class<?>[] paramTypes, Object... args) throws Exception {
        Method m = RemoteClientServerZKRESTProxy.class.getDeclaredMethod(methodName, paramTypes);
        m.setAccessible(true);
        return m.invoke(proxy, args);
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
    void testGetModelState_unknownModelDefaultsToCold() {
        assertEquals(RemoteModelStateEnum.COLD, proxy.getModelState("nonexistent"));
    }

    @Test
    void testGetModelState_nullModelId_throwsNPE() {
        assertThrows(NullPointerException.class, () -> proxy.getModelState(null));
    }

    @Test
    void testGetModelState_emptyModelId() {
        assertEquals(RemoteModelStateEnum.COLD, proxy.getModelState(""));
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

    // ========== Tests: getModelClusterIp (cache hit) ==========

    @Test
    void testGetModelClusterIp_cached() throws Exception {
        getModelClusterIps().put("model-1", "10.0.0.1");
        assertEquals("10.0.0.1", proxy.getModelClusterIp("model-1"));
    }

    @Test
    void testGetModelClusterIp_multipleCached() throws Exception {
        getModelClusterIps().put("m1", "10.0.0.1");
        getModelClusterIps().put("m2", "10.0.0.2");
        assertEquals("10.0.0.1", proxy.getModelClusterIp("m1"));
        assertEquals("10.0.0.2", proxy.getModelClusterIp("m2"));
    }

    @Test
    void testGetModelClusterIp_cacheMiss_httpReturnsData() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        JSONObject data = new JSONObject();
        data.put("ip", "10.0.0.5");
        data.put("model_name", "TestModel");
        JSONObject responseBody = new JSONObject();
        responseBody.put("data", data);

        CloseableHttpResponse mockResponse = createMockResponse(200, responseBody.toString());
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            String ip = proxy.getModelClusterIp("model-new");
            assertEquals("10.0.0.5", ip);
            // Verify it also cached the results
            assertEquals("10.0.0.5", getModelClusterIps().get("model-new"));
            assertEquals("TestModel", getModelNames().get("model-new"));
        }
    }

    @Test
    void testGetModelClusterIp_cacheMiss_httpReturns404() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(404, null);
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            String ip = proxy.getModelClusterIp("missing-model");
            assertNull(ip);
        }
    }

    @Test
    void testGetModelClusterIp_cacheMiss_httpReturns500() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(500, null);
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            String ip = proxy.getModelClusterIp("error-model");
            assertNull(ip);
        }
    }

    @Test
    void testGetModelClusterIp_cacheMiss_httpThrowsException() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        when(mockClient.execute(any(HttpGet.class))).thenThrow(new IOException("Connection refused"));

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            String ip = proxy.getModelClusterIp("bad-model");
            assertNull(ip);
        }
    }

    // ========== Tests: getModelScalerIp ==========

    @Test
    void testGetModelScalerIp_doesNotThrow() {
        assertDoesNotThrow(() -> proxy.getModelScalerIp());
    }

    // ========== Tests: close ==========

    @Test
    void testClose_shutsDownScheduler() throws Exception {
        proxy.close();
        assertTrue(getScheduler().isShutdown());
    }

    @Test
    void testClose_calledTwice_noException() {
        proxy.close();
        assertDoesNotThrow(() -> proxy.close());
    }

    @Test
    void testClose_schedulerIsShutdownAfterClose() throws Exception {
        assertFalse(getScheduler().isShutdown());
        proxy.close();
        assertTrue(getScheduler().isShutdown());
    }

    // ========== Tests: isModelWarming (via checkZNodeExists) ==========

    @Test
    void testIsModelWarming_exists() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(200, null);
        when(mockClient.execute(any(HttpHead.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            assertTrue(proxy.isModelWarming("model-1"));
        }
    }

    @Test
    void testIsModelWarming_notExists() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(404, null);
        when(mockClient.execute(any(HttpHead.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            assertFalse(proxy.isModelWarming("model-1"));
        }
    }

    @Test
    void testIsModelWarming_serverError() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(500, null);
        when(mockClient.execute(any(HttpHead.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            assertFalse(proxy.isModelWarming("model-1"));
        }
    }

    @Test
    void testIsModelWarming_ioException() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        when(mockClient.execute(any(HttpHead.class))).thenThrow(new IOException("Timeout"));

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            assertFalse(proxy.isModelWarming("model-1"));
        }
    }

    // ========== Tests: isModelActive (via checkZNodeExists) ==========

    @Test
    void testIsModelActive_exists() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(200, null);
        when(mockClient.execute(any(HttpHead.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            assertTrue(proxy.isModelActive("model-1"));
        }
    }

    @Test
    void testIsModelActive_notExists() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(404, null);
        when(mockClient.execute(any(HttpHead.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            assertFalse(proxy.isModelActive("model-1"));
        }
    }

    @Test
    void testIsModelActive_ioException() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        when(mockClient.execute(any(HttpHead.class))).thenThrow(new IOException("Refused"));

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            assertFalse(proxy.isModelActive("model-1"));
        }
    }

    // ========== Tests: getZNodeDataAsJson (private) ==========

    @Test
    void testGetZNodeDataAsJson_success() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);

        JSONObject data = new JSONObject();
        data.put("ip", "10.0.0.1");
        data.put("model_name", "TestModel");
        JSONObject responseBody = new JSONObject();
        responseBody.put("data", data);

        CloseableHttpResponse mockResponse = createMockResponse(200, responseBody.toString());
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            JSONObject result = (JSONObject) invokePrivate("getZNodeDataAsJson",
                    new Class[]{String.class}, "/models/active/m1");
            assertNotNull(result);
            assertEquals("10.0.0.1", result.getString("ip"));
            assertEquals("TestModel", result.getString("model_name"));
        }
    }

    @Test
    void testGetZNodeDataAsJson_404() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(404, null);
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            JSONObject result = (JSONObject) invokePrivate("getZNodeDataAsJson",
                    new Class[]{String.class}, "/models/active/m1");
            assertNull(result);
        }
    }

    @Test
    void testGetZNodeDataAsJson_500() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(500, null);
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            JSONObject result = (JSONObject) invokePrivate("getZNodeDataAsJson",
                    new Class[]{String.class}, "/some/path");
            assertNull(result);
        }
    }

    @Test
    void testGetZNodeDataAsJson_nullDataField() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        JSONObject responseBody = new JSONObject();
        responseBody.put("data", JSONObject.NULL);

        CloseableHttpResponse mockResponse = createMockResponse(200, responseBody.toString());
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            JSONObject result = (JSONObject) invokePrivate("getZNodeDataAsJson",
                    new Class[]{String.class}, "/some/path");
            assertNull(result);
        }
    }

    @Test
    void testGetZNodeDataAsJson_noDataField() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        JSONObject responseBody = new JSONObject();
        responseBody.put("other", "value");

        CloseableHttpResponse mockResponse = createMockResponse(200, responseBody.toString());
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            JSONObject result = (JSONObject) invokePrivate("getZNodeDataAsJson",
                    new Class[]{String.class}, "/some/path");
            assertNull(result);
        }
    }

    @Test
    void testGetZNodeDataAsJson_nullEntity() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(200, null);
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            JSONObject result = (JSONObject) invokePrivate("getZNodeDataAsJson",
                    new Class[]{String.class}, "/some/path");
            assertNull(result);
        }
    }

    @Test
    void testGetZNodeDataAsJson_pathWithLeadingSlash() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);

        JSONObject data = new JSONObject();
        data.put("ip", "1.2.3.4");
        data.put("model_name", "Slash");
        JSONObject responseBody = new JSONObject();
        responseBody.put("data", data);

        CloseableHttpResponse mockResponse = createMockResponse(200, responseBody.toString());
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            JSONObject result = (JSONObject) invokePrivate("getZNodeDataAsJson",
                    new Class[]{String.class}, "/models/active/m1");
            assertNotNull(result);
            assertEquals("1.2.3.4", result.getString("ip"));
        }
    }

    @Test
    void testGetZNodeDataAsJson_pathWithoutLeadingSlash() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);

        JSONObject data = new JSONObject();
        data.put("ip", "5.6.7.8");
        data.put("model_name", "NoSlash");
        JSONObject responseBody = new JSONObject();
        responseBody.put("data", data);

        CloseableHttpResponse mockResponse = createMockResponse(200, responseBody.toString());
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            JSONObject result = (JSONObject) invokePrivate("getZNodeDataAsJson",
                    new Class[]{String.class}, "models/active/m1");
            assertNotNull(result);
            assertEquals("5.6.7.8", result.getString("ip"));
        }
    }

    @Test
    void testGetZNodeDataAsJson_ioException() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        when(mockClient.execute(any(HttpGet.class))).thenThrow(new IOException("Network error"));

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            JSONObject result = (JSONObject) invokePrivate("getZNodeDataAsJson",
                    new Class[]{String.class}, "/models/active/m1");
            assertNull(result);
        }
    }

    // ========== Tests: getZNodeData (private) ==========

    @Test
    void testGetZNodeData_success() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        JSONObject responseBody = new JSONObject();
        responseBody.put("data", "some-string-data");

        CloseableHttpResponse mockResponse = createMockResponse(200, responseBody.toString());
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            String result = (String) invokePrivate("getZNodeData",
                    new Class[]{String.class}, "/services/kube-model-deployer");
            assertEquals("some-string-data", result);
        }
    }

    @Test
    void testGetZNodeData_404() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(404, null);
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            String result = (String) invokePrivate("getZNodeData",
                    new Class[]{String.class}, "/some/path");
            assertNull(result);
        }
    }

    @Test
    void testGetZNodeData_500() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(500, null);
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            String result = (String) invokePrivate("getZNodeData",
                    new Class[]{String.class}, "/some/path");
            assertNull(result);
        }
    }

    @Test
    void testGetZNodeData_noDataField() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        JSONObject responseBody = new JSONObject();
        responseBody.put("other", "value");

        CloseableHttpResponse mockResponse = createMockResponse(200, responseBody.toString());
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            String result = (String) invokePrivate("getZNodeData",
                    new Class[]{String.class}, "/some/path");
            assertNull(result);
        }
    }

    @Test
    void testGetZNodeData_nullDataField() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        JSONObject responseBody = new JSONObject();
        responseBody.put("data", JSONObject.NULL);

        CloseableHttpResponse mockResponse = createMockResponse(200, responseBody.toString());
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            String result = (String) invokePrivate("getZNodeData",
                    new Class[]{String.class}, "/some/path");
            assertNull(result);
        }
    }

    @Test
    void testGetZNodeData_nullEntity() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(200, null);
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            String result = (String) invokePrivate("getZNodeData",
                    new Class[]{String.class}, "/some/path");
            assertNull(result);
        }
    }

    @Test
    void testGetZNodeData_ioException() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        when(mockClient.execute(any(HttpGet.class))).thenThrow(new IOException("Network error"));

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            String result = (String) invokePrivate("getZNodeData",
                    new Class[]{String.class}, "/some/path");
            assertNull(result);
        }
    }

    // ========== Tests: getZNodeChildren (private) ==========

    @Test
    void testGetZNodeChildren_success() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        JSONObject responseBody = new JSONObject();
        responseBody.put("children", new org.json.JSONArray(List.of("child1", "child2", "child3")));

        CloseableHttpResponse mockResponse = createMockResponse(200, responseBody.toString());
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            @SuppressWarnings("unchecked")
            List<String> result = (List<String>) invokePrivate("getZNodeChildren",
                    new Class[]{String.class}, "/models/active");
            assertEquals(3, result.size());
            assertTrue(result.contains("child1"));
            assertTrue(result.contains("child2"));
            assertTrue(result.contains("child3"));
        }
    }

    @Test
    void testGetZNodeChildren_emptyChildren() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        JSONObject responseBody = new JSONObject();
        responseBody.put("children", new org.json.JSONArray());

        CloseableHttpResponse mockResponse = createMockResponse(200, responseBody.toString());
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            @SuppressWarnings("unchecked")
            List<String> result = (List<String>) invokePrivate("getZNodeChildren",
                    new Class[]{String.class}, "/models/active");
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void testGetZNodeChildren_noChildrenField() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        JSONObject responseBody = new JSONObject();
        responseBody.put("data", "something");

        CloseableHttpResponse mockResponse = createMockResponse(200, responseBody.toString());
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            @SuppressWarnings("unchecked")
            List<String> result = (List<String>) invokePrivate("getZNodeChildren",
                    new Class[]{String.class}, "/models/active");
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void testGetZNodeChildren_404() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(404, null);
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            @SuppressWarnings("unchecked")
            List<String> result = (List<String>) invokePrivate("getZNodeChildren",
                    new Class[]{String.class}, "/models/active");
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void testGetZNodeChildren_500() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(500, null);
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            @SuppressWarnings("unchecked")
            List<String> result = (List<String>) invokePrivate("getZNodeChildren",
                    new Class[]{String.class}, "/models/active");
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void testGetZNodeChildren_ioException() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        when(mockClient.execute(any(HttpGet.class))).thenThrow(new IOException("Connection refused"));

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            @SuppressWarnings("unchecked")
            List<String> result = (List<String>) invokePrivate("getZNodeChildren",
                    new Class[]{String.class}, "/models/active");
            assertTrue(result.isEmpty());
        }
    }

    // ========== Tests: checkZNodeExists (private) ==========

    @Test
    void testCheckZNodeExists_true() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(200, null);
        when(mockClient.execute(any(HttpHead.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            boolean result = (boolean) invokePrivate("checkZNodeExists",
                    new Class[]{String.class}, "/models/warming/m1");
            assertTrue(result);
        }
    }

    @Test
    void testCheckZNodeExists_false_404() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(404, null);
        when(mockClient.execute(any(HttpHead.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            boolean result = (boolean) invokePrivate("checkZNodeExists",
                    new Class[]{String.class}, "/models/warming/m1");
            assertFalse(result);
        }
    }

    @Test
    void testCheckZNodeExists_false_serverError() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(500, null);
        when(mockClient.execute(any(HttpHead.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            boolean result = (boolean) invokePrivate("checkZNodeExists",
                    new Class[]{String.class}, "/models/warming/m1");
            assertFalse(result);
        }
    }

    @Test
    void testCheckZNodeExists_false_ioException() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        when(mockClient.execute(any(HttpHead.class))).thenThrow(new IOException("Timeout"));

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            boolean result = (boolean) invokePrivate("checkZNodeExists",
                    new Class[]{String.class}, "/models/warming/m1");
            assertFalse(result);
        }
    }

    @Test
    void testCheckZNodeExists_pathStripsLeadingSlash() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(200, null);
        when(mockClient.execute(any(HttpHead.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            boolean result = (boolean) invokePrivate("checkZNodeExists",
                    new Class[]{String.class}, "/test/path");
            assertTrue(result);
        }
    }

    // ========== Tests: getActiveModels ==========

    @Test
    void testGetActiveModels_withCachedNames() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);

        // getZNodeChildren returns two model IDs
        JSONObject childrenResponse = new JSONObject();
        childrenResponse.put("children", new org.json.JSONArray(List.of("m1", "m2")));
        CloseableHttpResponse childrenResp = createMockResponse(200, childrenResponse.toString());
        when(mockClient.execute(any(HttpGet.class))).thenReturn(childrenResp);

        // Pre-populate caches
        getModelNames().put("m1", "Model-A");
        getModelNames().put("m2", "Model-B");
        getModelStates().put("m1", RemoteModelStateEnum.ACTIVE);
        getModelStates().put("m2", RemoteModelStateEnum.ACTIVE);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            List<RemoteModelInfo> result = proxy.getActiveModels();
            assertEquals(2, result.size());
            assertEquals("m1", result.get(0).getId());
            assertEquals("Model-A", result.get(0).getName());
            assertEquals(RemoteModelStateEnum.ACTIVE, result.get(0).getState());
            assertEquals("m2", result.get(1).getId());
            assertEquals("Model-B", result.get(1).getName());
        }
    }

    @Test
    void testGetActiveModels_empty() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);

        JSONObject childrenResponse = new JSONObject();
        childrenResponse.put("children", new org.json.JSONArray());
        CloseableHttpResponse childrenResp = createMockResponse(200, childrenResponse.toString());
        when(mockClient.execute(any(HttpGet.class))).thenReturn(childrenResp);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            List<RemoteModelInfo> result = proxy.getActiveModels();
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void testGetActiveModels_unknownNameFallsBackToHttp() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);

        // First call: getZNodeChildren
        JSONObject childrenResponse = new JSONObject();
        childrenResponse.put("children", new org.json.JSONArray(List.of("m1")));
        CloseableHttpResponse childrenResp = createMockResponse(200, childrenResponse.toString());

        // Second call: getZNodeDataAsJson for m1
        JSONObject data = new JSONObject();
        data.put("ip", "10.0.0.1");
        data.put("model_name", "FetchedModel");
        JSONObject dataResponse = new JSONObject();
        dataResponse.put("data", data);
        CloseableHttpResponse dataResp = createMockResponse(200, dataResponse.toString());

        when(mockClient.execute(any(HttpGet.class)))
                .thenReturn(childrenResp)
                .thenReturn(dataResp);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            List<RemoteModelInfo> result = proxy.getActiveModels();
            assertEquals(1, result.size());
            assertEquals("m1", result.get(0).getId());
            assertEquals("FetchedModel", result.get(0).getName());
            // Verify it cached the name
            assertEquals("FetchedModel", getModelNames().get("m1"));
        }
    }

    @Test
    void testGetActiveModels_unknownNameAndHttpFails() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);

        JSONObject childrenResponse = new JSONObject();
        childrenResponse.put("children", new org.json.JSONArray(List.of("m1")));
        CloseableHttpResponse childrenResp = createMockResponse(200, childrenResponse.toString());

        CloseableHttpResponse dataResp = createMockResponse(404, null);

        when(mockClient.execute(any(HttpGet.class)))
                .thenReturn(childrenResp)
                .thenReturn(dataResp);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            List<RemoteModelInfo> result = proxy.getActiveModels();
            assertEquals(1, result.size());
            assertEquals("m1", result.get(0).getId());
            assertEquals("Unknown", result.get(0).getName());
        }
    }

    @Test
    void testGetActiveModels_httpExceptionReturnsEmptyList() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        when(mockClient.execute(any(HttpGet.class))).thenThrow(new IOException("Network error"));

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            List<RemoteModelInfo> result = proxy.getActiveModels();
            assertTrue(result.isEmpty());
        }
    }

    // ========== Tests: getWarmingModels ==========

    @Test
    void testGetWarmingModels_withCachedNames() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);

        JSONObject childrenResponse = new JSONObject();
        childrenResponse.put("children", new org.json.JSONArray(List.of("w1", "w2")));
        CloseableHttpResponse childrenResp = createMockResponse(200, childrenResponse.toString());
        when(mockClient.execute(any(HttpGet.class))).thenReturn(childrenResp);

        getModelNames().put("w1", "Warming-A");
        getModelStates().put("w1", RemoteModelStateEnum.WARMING);
        getModelStates().put("w2", RemoteModelStateEnum.WARMING);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            List<RemoteModelInfo> result = proxy.getWarmingModels();
            assertEquals(2, result.size());
            assertEquals("w1", result.get(0).getId());
            assertEquals("Warming-A", result.get(0).getName());
            assertEquals(RemoteModelStateEnum.WARMING, result.get(0).getState());
            assertEquals("w2", result.get(1).getId());
            assertEquals("Warming...", result.get(1).getName()); // Unknown name defaults
        }
    }

    @Test
    void testGetWarmingModels_empty() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);

        JSONObject childrenResponse = new JSONObject();
        childrenResponse.put("children", new org.json.JSONArray());
        CloseableHttpResponse childrenResp = createMockResponse(200, childrenResponse.toString());
        when(mockClient.execute(any(HttpGet.class))).thenReturn(childrenResp);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            List<RemoteModelInfo> result = proxy.getWarmingModels();
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void testGetWarmingModels_httpExceptionReturnsEmptyList() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        when(mockClient.execute(any(HttpGet.class))).thenThrow(new IOException("Network error"));

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            List<RemoteModelInfo> result = proxy.getWarmingModels();
            assertTrue(result.isEmpty());
        }
    }

    // ========== Tests: waitForModelActive ==========

    @Test
    void testWaitForModelActive_alreadyActive() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(200, null);
        when(mockClient.execute(any(HttpHead.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            assertTrue(proxy.waitForModelActive("model-1", 5000));
            assertEquals(RemoteModelStateEnum.ACTIVE, getModelStates().get("model-1"));
        }
    }

    @Test
    void testWaitForModelActive_timeout() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(404, null);
        when(mockClient.execute(any(HttpHead.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            // Use very short timeout to avoid long waits
            assertFalse(proxy.waitForModelActive("model-1", 100));
        }
    }

    // ========== Tests: waitForState ==========

    @Test
    void testWaitForState_alreadyInDesiredState() throws Exception {
        getModelStates().put("model-1", RemoteModelStateEnum.ACTIVE);
        assertTrue(proxy.waitForState("model-1", RemoteModelStateEnum.ACTIVE, 5000));
    }

    @Test
    void testWaitForState_waitingForCold_defaultsCold() {
        // Unknown model defaults to COLD
        assertTrue(proxy.waitForState("nonexistent", RemoteModelStateEnum.COLD, 5000));
    }

    @Test
    void testWaitForState_warmingModelWaitsForActive_timeout() throws Exception {
        getModelStates().put("model-1", RemoteModelStateEnum.WARMING);

        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(404, null);
        when(mockClient.execute(any(HttpHead.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            assertFalse(proxy.waitForState("model-1", RemoteModelStateEnum.ACTIVE, 100));
        }
    }

    @Test
    void testWaitForState_coldModelBecomesActive() throws Exception {
        // Model is COLD, but isModelActive returns true
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(200, null);
        when(mockClient.execute(any(HttpHead.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            assertTrue(proxy.waitForState("model-1", RemoteModelStateEnum.ACTIVE, 5000));
            assertEquals(RemoteModelStateEnum.ACTIVE, getModelStates().get("model-1"));
        }
    }

    // ========== Tests: refreshModelStates (private) ==========

    @Test
    void testRefreshModelStates_updatesStates() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);

        // First call: getZNodeChildren for ACTIVE_PATH returns ["m1"]
        JSONObject activeChildren = new JSONObject();
        activeChildren.put("children", new org.json.JSONArray(List.of("m1")));
        CloseableHttpResponse activeChildrenResp = createMockResponse(200, activeChildren.toString());

        // Second call: getZNodeChildren for WARMING_PATH returns ["m2"]
        JSONObject warmingChildren = new JSONObject();
        warmingChildren.put("children", new org.json.JSONArray(List.of("m2")));
        CloseableHttpResponse warmingChildrenResp = createMockResponse(200, warmingChildren.toString());

        // Third call: getZNodeDataAsJson for m1
        JSONObject data = new JSONObject();
        data.put("ip", "10.0.0.1");
        data.put("model_name", "Model-A");
        JSONObject dataResponse = new JSONObject();
        dataResponse.put("data", data);
        CloseableHttpResponse dataResp = createMockResponse(200, dataResponse.toString());

        when(mockClient.execute(any(HttpGet.class)))
                .thenReturn(activeChildrenResp)
                .thenReturn(warmingChildrenResp)
                .thenReturn(dataResp);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            invokePrivate("refreshModelStates", new Class[]{});
            assertEquals(RemoteModelStateEnum.ACTIVE, getModelStates().get("m1"));
            assertEquals(RemoteModelStateEnum.WARMING, getModelStates().get("m2"));
            assertEquals("10.0.0.1", getModelClusterIps().get("m1"));
            assertEquals("Model-A", getModelNames().get("m1"));
        }
    }

    @Test
    void testRefreshModelStates_staleEntriesBecomeCold() throws Exception {
        // Pre-populate with a model that won't appear in refresh
        getModelStates().put("stale-model", RemoteModelStateEnum.ACTIVE);

        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);

        JSONObject activeChildren = new JSONObject();
        activeChildren.put("children", new org.json.JSONArray());
        CloseableHttpResponse activeResp = createMockResponse(200, activeChildren.toString());

        JSONObject warmingChildren = new JSONObject();
        warmingChildren.put("children", new org.json.JSONArray());
        CloseableHttpResponse warmingResp = createMockResponse(200, warmingChildren.toString());

        when(mockClient.execute(any(HttpGet.class)))
                .thenReturn(activeResp)
                .thenReturn(warmingResp);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            invokePrivate("refreshModelStates", new Class[]{});
            assertEquals(RemoteModelStateEnum.COLD, getModelStates().get("stale-model"));
        }
    }

    @Test
    void testRefreshModelStates_activeOverridesWarming() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);

        // Model appears in both active and warming
        JSONObject activeChildren = new JSONObject();
        activeChildren.put("children", new org.json.JSONArray(List.of("m1")));
        CloseableHttpResponse activeResp = createMockResponse(200, activeChildren.toString());

        JSONObject warmingChildren = new JSONObject();
        warmingChildren.put("children", new org.json.JSONArray(List.of("m1")));
        CloseableHttpResponse warmingResp = createMockResponse(200, warmingChildren.toString());

        JSONObject data = new JSONObject();
        data.put("ip", "10.0.0.1");
        data.put("model_name", "Model-A");
        JSONObject dataResponse = new JSONObject();
        dataResponse.put("data", data);
        CloseableHttpResponse dataResp = createMockResponse(200, dataResponse.toString());

        when(mockClient.execute(any(HttpGet.class)))
                .thenReturn(activeResp)
                .thenReturn(warmingResp)
                .thenReturn(dataResp);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            invokePrivate("refreshModelStates", new Class[]{});
            // Should still be ACTIVE because active is processed first
            assertEquals(RemoteModelStateEnum.ACTIVE, getModelStates().get("m1"));
        }
    }

    // ========== Tests: updateModelInfoFromZNode (private) ==========

    @Test
    void testUpdateModelInfoFromZNode_success() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);

        JSONObject data = new JSONObject();
        data.put("ip", "10.0.0.99");
        data.put("model_name", "UpdatedModel");
        JSONObject responseBody = new JSONObject();
        responseBody.put("data", data);
        CloseableHttpResponse mockResponse = createMockResponse(200, responseBody.toString());
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            invokePrivate("updateModelInfoFromZNode",
                    new Class[]{String.class, String.class}, "m1", "/models/active/m1");
            assertEquals("10.0.0.99", getModelClusterIps().get("m1"));
            assertEquals("UpdatedModel", getModelNames().get("m1"));
        }
    }

    @Test
    void testUpdateModelInfoFromZNode_nullData() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(404, null);
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            invokePrivate("updateModelInfoFromZNode",
                    new Class[]{String.class, String.class}, "m1", "/models/active/m1");
            assertNull(getModelClusterIps().get("m1"));
            assertNull(getModelNames().get("m1"));
        }
    }

    // ========== Tests: validateConnection (private) ==========

    @Test
    void testValidateConnection_success() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(200, null);
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            assertDoesNotThrow(() -> invokePrivate("validateConnection", new Class[]{}));
        }
    }

    @Test
    void testValidateConnection_nonOkStatus() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = createMockResponse(503, null);
        when(mockClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            Exception ex = assertThrows(Exception.class,
                    () -> invokePrivate("validateConnection", new Class[]{}));
            assertTrue(ex.getCause().getMessage().contains("health check failed"));
        }
    }

    @Test
    void testValidateConnection_ioException() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        when(mockClient.execute(any(HttpGet.class))).thenThrow(new IOException("Connection refused"));

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            Exception ex = assertThrows(Exception.class,
                    () -> invokePrivate("validateConnection", new Class[]{}));
            assertTrue(ex.getCause().getMessage().contains("Failed to connect"));
        }
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

        assertEquals("10.0.0.1", proxy.getModelClusterIp("m1"));
        assertEquals("10.0.0.2", proxy.getModelClusterIp("m2"));

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

    // ========== Tests: loadInitialState (private) ==========

    @Test
    void testLoadInitialState_loadsActiveAndWarming() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);

        // First call: getZNodeData for MODEL_SCALER_PATH
        JSONObject scalerResponse = new JSONObject();
        scalerResponse.put("data", "10.0.0.100");
        CloseableHttpResponse scalerResp = createMockResponse(200, scalerResponse.toString());

        // Second call: getZNodeChildren for ACTIVE_PATH
        JSONObject activeChildren = new JSONObject();
        activeChildren.put("children", new org.json.JSONArray(List.of("a1")));
        CloseableHttpResponse activeChildrenResp = createMockResponse(200, activeChildren.toString());

        // Third call: getZNodeDataAsJson for a1
        JSONObject a1Data = new JSONObject();
        a1Data.put("ip", "10.0.0.10");
        a1Data.put("model_name", "ActiveModel");
        JSONObject a1Response = new JSONObject();
        a1Response.put("data", a1Data);
        CloseableHttpResponse a1DataResp = createMockResponse(200, a1Response.toString());

        // Fourth call: getZNodeChildren for WARMING_PATH
        JSONObject warmingChildren = new JSONObject();
        warmingChildren.put("children", new org.json.JSONArray(List.of("w1")));
        CloseableHttpResponse warmingChildrenResp = createMockResponse(200, warmingChildren.toString());

        when(mockClient.execute(any(HttpGet.class)))
                .thenReturn(scalerResp)
                .thenReturn(activeChildrenResp)
                .thenReturn(a1DataResp)
                .thenReturn(warmingChildrenResp);

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            invokePrivate("loadInitialState", new Class[]{});

            assertEquals(RemoteModelStateEnum.ACTIVE, getModelStates().get("a1"));
            assertEquals("10.0.0.10", getModelClusterIps().get("a1"));
            assertEquals("ActiveModel", getModelNames().get("a1"));
            assertEquals(RemoteModelStateEnum.WARMING, getModelStates().get("w1"));
        }
    }

    @Test
    void testLoadInitialState_httpError_doesNotThrow() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        when(mockClient.execute(any(HttpGet.class))).thenThrow(new IOException("Network error"));

        try (MockedStatic<HttpClients> mockedHttp = setupHttpMock(mockClient)) {
            assertDoesNotThrow(() -> invokePrivate("loadInitialState", new Class[]{}));
        }
    }
}