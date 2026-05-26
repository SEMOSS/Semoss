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

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sun.misc.Unsafe;

class ClusterSynchronizerUnitTests {

    @Nested
    class ConstantsTests {
        @Test void testZkServerStringConstant() { assertEquals("ZK_SERVER", ClusterSynchronizer.ZK_SERVER_STRING); }
        @Test void testHostIpConstant() { assertEquals("HOST_IP", ClusterSynchronizer.HOST_IP); }
        @Test void testSyncProjectPathConstant() { assertEquals("/sync/project", ClusterSynchronizer.SYNC_PROJECT_PATH); }
        @Test void testSyncEnginePathConstant() { assertEquals("/sync/engine", ClusterSynchronizer.SYNC_ENGINE_PATH); }
    }

    @Nested
    class UnsafeInstanceTests {
        private ClusterSynchronizer instance;
        private CuratorFramework mockClient;

        @BeforeEach
        void setUp() throws Exception {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Unsafe unsafe = (Unsafe) unsafeField.get(null);
            instance = (ClusterSynchronizer) unsafe.allocateInstance(ClusterSynchronizer.class);

            mockClient = mock(CuratorFramework.class, RETURNS_DEEP_STUBS);
            Field clientField = ClusterSynchronizer.class.getDeclaredField("client");
            clientField.setAccessible(true);
            clientField.set(instance, mockClient);

            Field hostField = ClusterSynchronizer.class.getDeclaredField("host");
            hostField.setAccessible(true);
            hostField.set(instance, "test-host");
        }

        @Test void testHostFieldDefaultNullAfterUnsafe() throws Exception {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Unsafe unsafe = (Unsafe) unsafeField.get(null);
            ClusterSynchronizer raw = (ClusterSynchronizer) unsafe.allocateInstance(ClusterSynchronizer.class);
            Field hostField = ClusterSynchronizer.class.getDeclaredField("host");
            hostField.setAccessible(true);
            assertNull(hostField.get(raw));
        }

        @Test void testClientFieldDefaultNullAfterUnsafe() throws Exception {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Unsafe unsafe = (Unsafe) unsafeField.get(null);
            ClusterSynchronizer raw = (ClusterSynchronizer) unsafe.allocateInstance(ClusterSynchronizer.class);
            Field clientField = ClusterSynchronizer.class.getDeclaredField("client");
            clientField.setAccessible(true);
            assertNull(clientField.get(raw));
        }

        @Test void testPublishEngineChange_pathDoesNotExist_createsPath() throws Exception {
            when(mockClient.checkExists().forPath(anyString())).thenReturn(null);
            when(mockClient.setData().forPath(anyString(), any(byte[].class))).thenReturn(new Stat());
            instance.publishEngineChange("engine1", "pullOwl", "param1");
            verify(mockClient.checkExists()).forPath(eq("/sync/engine/engine1"));
            verify(mockClient.create().creatingParentsIfNeeded()).forPath(eq("/sync/engine/engine1"));
        }

        @Test void testPublishEngineChange_pathExists_doesNotCreate() throws Exception {
            when(mockClient.checkExists().forPath(anyString())).thenReturn(new Stat());
            when(mockClient.setData().forPath(anyString(), any(byte[].class))).thenReturn(new Stat());
            instance.publishEngineChange("engine1", "pullOwl", "param1");
            verify(mockClient.create().creatingParentsIfNeeded(), never()).forPath(anyString());
        }

        @Test void testPublishEngineChange_setsSerializedData() throws Exception {
            when(mockClient.checkExists().forPath(anyString())).thenReturn(new Stat());
            when(mockClient.setData().forPath(anyString(), any(byte[].class))).thenReturn(new Stat());
            instance.publishEngineChange("engine1", "pullOwl", "param1", "param2");
            verify(mockClient.setData()).forPath(eq("/sync/engine/engine1"), any(byte[].class));
        }

        @Test void testPublishProjectChange_pathDoesNotExist_createsPath() throws Exception {
            when(mockClient.checkExists().forPath(anyString())).thenReturn(null);
            when(mockClient.setData().forPath(anyString(), any(byte[].class))).thenReturn(new Stat());
            instance.publishProjectChange("project1", "pullInsightDB", "p1");
            verify(mockClient.checkExists()).forPath(eq("/sync/project/project1"));
            verify(mockClient.create().creatingParentsIfNeeded()).forPath(eq("/sync/project/project1"));
        }

        @Test void testPublishProjectChange_pathExists_doesNotCreate() throws Exception {
            when(mockClient.checkExists().forPath(anyString())).thenReturn(new Stat());
            when(mockClient.setData().forPath(anyString(), any(byte[].class))).thenReturn(new Stat());
            instance.publishProjectChange("project1", "pullInsightDB", "p1");
            verify(mockClient.create().creatingParentsIfNeeded(), never()).forPath(anyString());
        }

        @Test void testPublishProjectChange_setsSerializedData() throws Exception {
            when(mockClient.checkExists().forPath(anyString())).thenReturn(new Stat());
            when(mockClient.setData().forPath(anyString(), any(byte[].class))).thenReturn(new Stat());
            instance.publishProjectChange("project1", "pullInsightDB", "p1");
            verify(mockClient.setData()).forPath(eq("/sync/project/project1"), any(byte[].class));
        }

        @Test void testPublishEngineChange_serializedDataContainsNodeId() throws Exception {
            when(mockClient.checkExists().forPath(anyString())).thenReturn(new Stat());
            final byte[][] captured = new byte[1][];
            when(mockClient.setData().forPath(anyString(), any(byte[].class))).thenAnswer(inv -> {
                captured[0] = inv.getArgument(1);
                return new Stat();
            });
            instance.publishEngineChange("eng1", "pullOwl", "p1");
            assertNotNull(captured[0]);
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(captured[0]));
            @SuppressWarnings("unchecked")
            Map<String, Object> dataMap = (Map<String, Object>) ois.readObject();
            assertEquals("test-host", dataMap.get("nodeId"));
            assertEquals("pullOwl", dataMap.get("methodName"));
            @SuppressWarnings("unchecked")
            List<Object> params = (List<Object>) dataMap.get("params");
            assertEquals(1, params.size());
            assertEquals("p1", params.get(0));
        }

        @Test void testPublishProjectChange_serializedDataContainsNodeId() throws Exception {
            when(mockClient.checkExists().forPath(anyString())).thenReturn(new Stat());
            final byte[][] captured = new byte[1][];
            when(mockClient.setData().forPath(anyString(), any(byte[].class))).thenAnswer(inv -> {
                captured[0] = inv.getArgument(1);
                return new Stat();
            });
            instance.publishProjectChange("proj1", "pushInsightDB", "x", "y");
            assertNotNull(captured[0]);
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(captured[0]));
            @SuppressWarnings("unchecked")
            Map<String, Object> dataMap = (Map<String, Object>) ois.readObject();
            assertEquals("test-host", dataMap.get("nodeId"));
            assertEquals("pushInsightDB", dataMap.get("methodName"));
            @SuppressWarnings("unchecked")
            List<Object> params = (List<Object>) dataMap.get("params");
            assertEquals(2, params.size());
            assertEquals("x", params.get(0));
            assertEquals("y", params.get(1));
        }

        @Test void testPublishEngineChange_noParams() throws Exception {
            when(mockClient.checkExists().forPath(anyString())).thenReturn(new Stat());
            final byte[][] captured = new byte[1][];
            when(mockClient.setData().forPath(anyString(), any(byte[].class))).thenAnswer(inv -> {
                captured[0] = inv.getArgument(1);
                return new Stat();
            });
            instance.publishEngineChange("eng1", "someMethod");
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(captured[0]));
            @SuppressWarnings("unchecked")
            Map<String, Object> dataMap = (Map<String, Object>) ois.readObject();
            @SuppressWarnings("unchecked")
            List<Object> params = (List<Object>) dataMap.get("params");
            assertTrue(params.isEmpty());
        }
    }
}
