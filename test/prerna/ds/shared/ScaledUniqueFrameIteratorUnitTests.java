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
package prerna.ds.shared;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;

public class ScaledUniqueFrameIteratorUnitTests {
    IRawSelectWrapper it;
    ITableDataFrame frame;
    IHeadersDataRow dataRow;
    ScaledUniqueFrameIterator reactor;

    @BeforeEach
    void setup() {
        it = mock(IRawSelectWrapper.class);
        frame = mock(ITableDataFrame.class);
        dataRow = mock(IHeadersDataRow.class);
    }

    @Test
    void test() throws Exception {
        String col = "col";
        Double[] maxArr = new Double[]{10.0, null};
        Double[] minArr = new Double[]{1.0, null};
        Object[] obj = new Object[]{1, ""};
        List<SemossDataType> types = new ArrayList<>();
        List<String> selectors = new ArrayList<>();
        selectors.add("tableName__col");
        selectors.add("tableName");

        when(frame.getColumn(col)).thenReturn(obj);
        when(frame.query(any(SelectQueryStruct.class))).thenReturn(it).thenThrow(Exception.class);
        when(it.hasNext()).thenReturn(true).thenReturn(false);
        when(it.next()).thenReturn(dataRow);
        when(dataRow.getValues()).thenReturn(null);
        doThrow(IOException.class).doNothing().when(it).close();

        reactor = new ScaledUniqueFrameIterator(frame, col, maxArr, minArr, types, selectors);
        assertNotNull(reactor);

        List<Object[]> expected = new ArrayList<>();
        expected.add(null);
        assertEquals(expected, reactor.next());

        expected = new ArrayList<>();
        assertEquals(expected, reactor.next());
        
        NoSuchElementException e = assertThrows(NoSuchElementException.class, () -> reactor.next());
        assertEquals("No more elements", e.getMessage());
    }
}
