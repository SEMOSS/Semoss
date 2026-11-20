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

import org.junit.jupiter.api.Test;

import prerna.engine.api.RemoteModelStateEnum;

class RemoteModelInfoUnitTests {

    @Test
    void testConstructorAndGetters() {
        RemoteModelInfo info = new RemoteModelInfo("id1", "modelName", RemoteModelStateEnum.ACTIVE);
        assertEquals("id1", info.getId());
        assertEquals("modelName", info.getName());
        assertEquals(RemoteModelStateEnum.ACTIVE, info.getState());
    }

    @Test
    void testStateCold() {
        RemoteModelInfo info = new RemoteModelInfo("c1", "cold-model", RemoteModelStateEnum.COLD);
        assertEquals(RemoteModelStateEnum.COLD, info.getState());
        assertEquals("c1", info.getId());
        assertEquals("cold-model", info.getName());
    }

    @Test
    void testStateWarming() {
        RemoteModelInfo info = new RemoteModelInfo("w1", "warming-model", RemoteModelStateEnum.WARMING);
        assertEquals(RemoteModelStateEnum.WARMING, info.getState());
        assertEquals("w1", info.getId());
        assertEquals("warming-model", info.getName());
    }

    @Test
    void testStateActive() {
        RemoteModelInfo info = new RemoteModelInfo("a1", "active-model", RemoteModelStateEnum.ACTIVE);
        assertEquals(RemoteModelStateEnum.ACTIVE, info.getState());
        assertEquals("a1", info.getId());
        assertEquals("active-model", info.getName());
    }

    @Test
    void testStateFailed() {
        RemoteModelInfo info = new RemoteModelInfo("f1", "failed-model", RemoteModelStateEnum.FAILED);
        assertEquals(RemoteModelStateEnum.FAILED, info.getState());
        assertEquals("f1", info.getId());
        assertEquals("failed-model", info.getName());
    }

    @Test
    void testStateUnknown() {
        RemoteModelInfo info = new RemoteModelInfo("u1", "unknown-model", RemoteModelStateEnum.UNKNOWN);
        assertEquals(RemoteModelStateEnum.UNKNOWN, info.getState());
        assertEquals("u1", info.getId());
        assertEquals("unknown-model", info.getName());
    }

    @Test
    void testNullId() {
        RemoteModelInfo info = new RemoteModelInfo(null, "name", RemoteModelStateEnum.ACTIVE);
        assertNull(info.getId());
        assertEquals("name", info.getName());
        assertEquals(RemoteModelStateEnum.ACTIVE, info.getState());
    }

    @Test
    void testNullName() {
        RemoteModelInfo info = new RemoteModelInfo("id", null, RemoteModelStateEnum.ACTIVE);
        assertEquals("id", info.getId());
        assertNull(info.getName());
        assertEquals(RemoteModelStateEnum.ACTIVE, info.getState());
    }

    @Test
    void testNullState() {
        RemoteModelInfo info = new RemoteModelInfo("id", "name", null);
        assertEquals("id", info.getId());
        assertEquals("name", info.getName());
        assertNull(info.getState());
    }

    @Test
    void testAllNulls() {
        RemoteModelInfo info = new RemoteModelInfo(null, null, null);
        assertNull(info.getId());
        assertNull(info.getName());
        assertNull(info.getState());
    }

    @Test
    void testEmptyStringId() {
        RemoteModelInfo info = new RemoteModelInfo("", "name", RemoteModelStateEnum.COLD);
        assertEquals("", info.getId());
        assertEquals("name", info.getName());
    }

    @Test
    void testEmptyStringName() {
        RemoteModelInfo info = new RemoteModelInfo("id", "", RemoteModelStateEnum.COLD);
        assertEquals("id", info.getId());
        assertEquals("", info.getName());
    }

    @Test
    void testEmptyStringsIdAndName() {
        RemoteModelInfo info = new RemoteModelInfo("", "", RemoteModelStateEnum.WARMING);
        assertEquals("", info.getId());
        assertEquals("", info.getName());
        assertEquals(RemoteModelStateEnum.WARMING, info.getState());
    }

    @Test
    void testAllEnumValuesExist() {
        RemoteModelStateEnum[] values = RemoteModelStateEnum.values();
        assertEquals(5, values.length, "Expected exactly 5 enum values");
    }
}
