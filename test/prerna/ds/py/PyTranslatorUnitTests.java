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
package prerna.ds.py;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import prerna.algorithm.api.SemossDataType;
import prerna.om.Insight;
import prerna.tcp.client.SocketClient;

class PyTranslatorUnitTests {

    @Nested
    class StaticMapTests {
        @Test
        void pyS_object_mapsToString() { assertEquals(SemossDataType.STRING, PyTranslator.pyS.get("object")); }
        @Test
        void pyS_category_mapsToString() { assertEquals(SemossDataType.STRING, PyTranslator.pyS.get("category")); }
        @Test
        void pyS_int64_mapsToInt() { assertEquals(SemossDataType.INT, PyTranslator.pyS.get("int64")); }
        @Test
        void pyS_float64_mapsToDouble() { assertEquals(SemossDataType.DOUBLE, PyTranslator.pyS.get("float64")); }
        @Test
        void pyS_datetime64_mapsToDate() { assertEquals(SemossDataType.DATE, PyTranslator.pyS.get("datetime64")); }
        @Test
        void pyS_datetime64ns_mapsToTimestamp() { assertEquals(SemossDataType.TIMESTAMP, PyTranslator.pyS.get("datetime64[ns]")); }
        @Test
        void pyS_unknown_returnsNull() { assertNull(PyTranslator.pyS.get("unknown")); }
        @Test
        void pyS_hasSixEntries() { assertEquals(6, PyTranslator.pyS.size()); }
    }

    @Nested
    class ConstructorAndGetterTests {
        @Test
        void constructor_setsFields() {
            SocketClient sc = mock(SocketClient.class);
            Insight insight = mock(Insight.class);
            PyTranslator pt = new PyTranslator(sc, insight);
            assertSame(sc, pt.getSocketClient());
            assertSame(insight, pt.getGlobalStoreInsight());
        }

        @Test
        void setSocketClient_updatesField() {
            SocketClient sc1 = mock(SocketClient.class);
            SocketClient sc2 = mock(SocketClient.class);
            Insight insight = mock(Insight.class);
            PyTranslator pt = new PyTranslator(sc1, insight);
            pt.setSocketClient(sc2);
            assertSame(sc2, pt.getSocketClient());
        }
    }

    @Nested
    class ConvertDataTypeTests {
        @Test
        void convertDataType_object() {
            SocketClient sc = mock(SocketClient.class);
            Insight insight = mock(Insight.class);
            PyTranslator pt = new PyTranslator(sc, insight);
            assertEquals(SemossDataType.STRING, pt.convertDataType("object"));
        }
        @Test
        void convertDataType_int64() {
            SocketClient sc = mock(SocketClient.class);
            Insight insight = mock(Insight.class);
            PyTranslator pt = new PyTranslator(sc, insight);
            assertEquals(SemossDataType.INT, pt.convertDataType("int64"));
        }
        @Test
        void convertDataType_float64() {
            SocketClient sc = mock(SocketClient.class);
            Insight insight = mock(Insight.class);
            PyTranslator pt = new PyTranslator(sc, insight);
            assertEquals(SemossDataType.DOUBLE, pt.convertDataType("float64"));
        }
        @Test
        void convertDataType_unknown_returnsNull() {
            SocketClient sc = mock(SocketClient.class);
            Insight insight = mock(Insight.class);
            PyTranslator pt = new PyTranslator(sc, insight);
            assertNull(pt.convertDataType("bool"));
        }
    }

    @Test
    void curEncoding_initiallyNull() {
        assertNull(PyTranslator.curEncoding);
    }
}
