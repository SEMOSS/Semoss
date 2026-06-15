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
package prerna.engine.impl.model.kserve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import prerna.engine.impl.model.kserve.KServeAdapter;

public class KServeAdapterUnitTests {
    private KServeAdapter classToTest;

    @BeforeEach
    public void setUp() {
        classToTest = new KServeAdapter();
    }

    @Test
    public void executeRequest() throws Exception{        
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("model", "");
        jsonObj.put("string", "value");
        jsonObj.put("JSONArray", new ArrayList(){{add(1);}});

        JSONObject retVal = classToTest.toKServeRequest(jsonObj);

        assertNotNull(retVal);
        assertEquals(2, retVal.getJSONArray("inputs").length());
        
        assertEquals(1, ((int[]) retVal.getJSONArray("inputs").getJSONObject(0).get("shape"))[0]);
        assertEquals("value", retVal.getJSONArray("inputs").getJSONObject(0).getJSONArray("data").getString(0));
        assertEquals("BYTES", retVal.getJSONArray("inputs").getJSONObject(0).get("datatype"));
        assertEquals("string", retVal.getJSONArray("inputs").getJSONObject(0).get("name"));

        assertEquals(1, ((int[]) retVal.getJSONArray("inputs").getJSONObject(1).get("shape"))[0]);
        assertEquals(1, retVal.getJSONArray("inputs").getJSONObject(1).getJSONArray("data").getInt(0));
        assertEquals("BYTES", retVal.getJSONArray("inputs").getJSONObject(1).get("datatype"));
        assertEquals("JSONArray", retVal.getJSONArray("inputs").getJSONObject(1).get("name"));
    }

    @Test
    public void executeRequestList() throws Exception{
        JSONObject mock = mock(JSONObject.class);

        Set<String> set = new HashSet<>();
        set.add("test");
        when(mock.keySet()).thenReturn(set);

        List<String> list = new ArrayList<>();
        list.add("value");
        when(mock.get("test")).thenReturn(list);
        
        JSONObject retVal = classToTest.toKServeRequest(mock);

        assertNotNull(retVal);
        assertEquals(1, retVal.getJSONArray("inputs").length());
        assertEquals(1, ((int[]) retVal.getJSONArray("inputs").getJSONObject(0).get("shape"))[0]);
        assertEquals("value", retVal.getJSONArray("inputs").getJSONObject(0).getJSONArray("data").getString(0));
        assertEquals("BYTES", retVal.getJSONArray("inputs").getJSONObject(0).get("datatype"));
        assertEquals("test", retVal.getJSONArray("inputs").getJSONObject(0).get("name"));
    }

    @Test
    public void executeResponse() {
        ArrayList<Object> list = new ArrayList<>();

        list.add(new JSONObject("{name:nameVal1, data:[]}"));
        list.add(new JSONObject("{name:nameVal2, data:[val2]}"));
        list.add(new JSONObject("{name:nameVal3, data:[\"{val3}\"]}"));
        list.add(new JSONObject("{name:nameVal4, data:[\"{val4:1}\"]}"));
        list.add(new JSONObject("{name:nameVal5, data:[\"[val5]\"]}"));
        list.add(new JSONObject("{name:nameVal6, data:['val6', 'val']}"));

        JSONObject kserveResponse = new JSONObject();
        kserveResponse.put("outputs", list);

        JSONObject retVal = classToTest.formatKServeResponse(kserveResponse);

        assertNotNull(retVal);
        assertEquals(6, retVal.length());

        assertEquals("[]", retVal.get("nameVal1").toString());
        assertEquals("val2", retVal.get("nameVal2").toString());
        assertEquals("{val3}", retVal.get("nameVal3").toString());
        assertEquals("{\"val4\":1}", retVal.get("nameVal4").toString());
        assertEquals("[\"val5\"]", retVal.get("nameVal5").toString());
        assertEquals("[\"val6\",\"val\"]", retVal.get("nameVal6").toString());
    }
}
