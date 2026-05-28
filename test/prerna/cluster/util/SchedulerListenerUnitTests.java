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

import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class SchedulerListenerUnitTests {

    @Nested
    class ConstantsAndFieldTests {
        @Test void testLeaderElectionRootNodeValue() {
            assertEquals("/election", SchedulerListener.LEADER_ELECTION_ROOT_NODE);
        }
        @Test void testProcessNodePrefixValue() throws Exception {
            Field f = SchedulerListener.class.getDeclaredField("PROCESS_NODE_PREFIX");
            f.setAccessible(true);
            assertEquals("/p_", f.get(null));
        }
        @Test void testSchedulerLeaderFieldType() throws Exception {
            Field f = SchedulerListener.class.getDeclaredField("schedulerLeader");
            assertEquals(boolean.class, f.getType());
        }
        @Test void testImplementsIZKListener() {
            assertTrue(IZKListener.class.isAssignableFrom(SchedulerListener.class));
        }
    }

    @Nested
    class ReflectionInstanceTests {
        private SchedulerListener listener;

        @BeforeEach void setUp() throws Exception {
            Constructor<SchedulerListener> ctor = SchedulerListener.class.getDeclaredConstructor();
            ctor.setAccessible(true);
            listener = ctor.newInstance();
        }

        @Test void testWatchedNodePathDefaultNull() throws Exception {
            Field f = SchedulerListener.class.getDeclaredField("watchedNodePath");
            f.setAccessible(true);
            assertNull(f.get(listener));
        }
        @Test void testProcessNodePathDefaultNull() throws Exception {
            Field f = SchedulerListener.class.getDeclaredField("processNodePath");
            f.setAccessible(true);
            assertNull(f.get(listener));
        }
        @Test void testIdDefaultNull() throws Exception {
            Field f = SchedulerListener.class.getDeclaredField("id");
            f.setAccessible(true);
            assertNull(f.get(listener));
        }
        @Test void testProcess_nonMatchingPath_doesNotThrow() throws Exception {
            Field wpField = SchedulerListener.class.getDeclaredField("watchedNodePath");
            wpField.setAccessible(true);
            wpField.set(listener, "/election/p_0001");
            assertDoesNotThrow(() -> listener.process("/some/other/path", mock(ZooKeeper.class)));
        }
        @Test void testProcess_nullWatchedNodePath_doesNotThrow() {
            assertDoesNotThrow(() -> listener.process("/some/path", mock(ZooKeeper.class)));
        }
    }

    @Nested
    class ProcessWithMockedZKClientTests {
        private SchedulerListener listener;

        @BeforeEach void setUp() throws Exception {
            Constructor<SchedulerListener> ctor = SchedulerListener.class.getDeclaredConstructor();
            ctor.setAccessible(true);
            listener = ctor.newInstance();
        }

        private void setSingleton() throws Exception {
            Field sf = SchedulerListener.class.getDeclaredField("schedulerListener");
            sf.setAccessible(true);
            sf.set(null, listener);
        }

        private void clearSingleton(Object orig) throws Exception {
            Field sf = SchedulerListener.class.getDeclaredField("schedulerListener");
            sf.setAccessible(true);
            sf.set(null, orig);
        }

        private Object getSingleton() throws Exception {
            Field sf = SchedulerListener.class.getDeclaredField("schedulerListener");
            sf.setAccessible(true);
            return sf.get(null);
        }

        @Test void testProcess_matchingPath_becomesLeader() throws Exception {
            Field wpField = SchedulerListener.class.getDeclaredField("watchedNodePath");
            wpField.setAccessible(true);
            wpField.set(listener, "/election/p_0001");
            Field ppField = SchedulerListener.class.getDeclaredField("processNodePath");
            ppField.setAccessible(true);
            ppField.set(listener, "/election/p_0001");
            Object orig = getSingleton();
            try (MockedStatic<ZKClient> zkMock = mockStatic(ZKClient.class)) {
                ZKClient mockZKClient = mock(ZKClient.class);
                zkMock.when(ZKClient::getInstance).thenReturn(mockZKClient);
                when(mockZKClient.getChildren(anyString(), anyBoolean()))
                    .thenReturn(Arrays.asList("p_0001", "p_0002"));
                setSingleton();
                listener.process("/election/p_0001", mock(ZooKeeper.class));
                assertTrue(SchedulerListener.schedulerLeader);
            } finally { SchedulerListener.schedulerLeader = false; clearSingleton(orig); }
        }

        @Test void testProcess_matchingPath_notFirst_setsWatch() throws Exception {
            Field wpField = SchedulerListener.class.getDeclaredField("watchedNodePath");
            wpField.setAccessible(true);
            wpField.set(listener, "/election/p_0001");
            Field ppField = SchedulerListener.class.getDeclaredField("processNodePath");
            ppField.setAccessible(true);
            ppField.set(listener, "/election/p_0002");
            Object orig = getSingleton();
            try (MockedStatic<ZKClient> zkMock = mockStatic(ZKClient.class)) {
                ZKClient mockZKClient = mock(ZKClient.class);
                zkMock.when(ZKClient::getInstance).thenReturn(mockZKClient);
                when(mockZKClient.getChildren(anyString(), anyBoolean()))
                    .thenReturn(Arrays.asList("p_0001", "p_0002"));
                setSingleton();
                listener.process("/election/p_0001", mock(ZooKeeper.class));
                assertFalse(SchedulerListener.schedulerLeader);
                verify(mockZKClient).watchEvent(eq("/election/p_0001"), any(), any(), eq(false));
            } finally { SchedulerListener.schedulerLeader = false; clearSingleton(orig); }
        }

        @Test void testProcess_matchingPath_updatesWatchedNodePath() throws Exception {
            Field wpField = SchedulerListener.class.getDeclaredField("watchedNodePath");
            wpField.setAccessible(true);
            wpField.set(listener, "/election/p_0001");
            Field ppField = SchedulerListener.class.getDeclaredField("processNodePath");
            ppField.setAccessible(true);
            ppField.set(listener, "/election/p_0002");
            Object orig = getSingleton();
            try (MockedStatic<ZKClient> zkMock = mockStatic(ZKClient.class)) {
                ZKClient mockZKClient = mock(ZKClient.class);
                zkMock.when(ZKClient::getInstance).thenReturn(mockZKClient);
                when(mockZKClient.getChildren(anyString(), anyBoolean()))
                    .thenReturn(Arrays.asList("p_0001", "p_0002"));
                setSingleton();
                listener.process("/election/p_0001", mock(ZooKeeper.class));
                assertEquals("/election/p_0001", wpField.get(listener));
            } finally { SchedulerListener.schedulerLeader = false; clearSingleton(orig); }
        }

        @Test void testSchedulerLeader_canBeToggled() {
            SchedulerListener.schedulerLeader = true;
            assertTrue(SchedulerListener.schedulerLeader);
            SchedulerListener.schedulerLeader = false;
            assertFalse(SchedulerListener.schedulerLeader);
        }
    }
}
