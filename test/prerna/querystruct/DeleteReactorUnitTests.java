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
package prerna.querystruct;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.delete.DeleteReactor;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.ReactorKeysEnum;

public class DeleteReactorUnitTests {
  private DeleteReactor reactor;

  @Mock private NounStore mockStore;

  @Mock private GenRowStruct mockTabGrs;

  @Mock private SelectQueryStruct selectQueryStruct;

  @BeforeEach
  void setup() {
    MockitoAnnotations.openMocks(this);
    reactor = new DeleteReactor();
    reactor.setNounStore(mockStore);
    when(mockStore.getNoun(ReactorKeysEnum.COLUMNS.getKey())).thenReturn(mockTabGrs);
  }

  @Test
  public void testCreateQueryStruct() {
    // Setup
    mockTabGrs = mock(GenRowStruct.class);
    when(mockStore.getNoun("from")).thenReturn(mockTabGrs);
    when(mockTabGrs.get(0)).thenReturn("columnName");

    // Execute
    AbstractQueryStruct result = reactor.createQueryStruct();

    // Verify
    assertNotNull(result);
    assertTrue(result instanceof SelectQueryStruct);
    SelectQueryStruct qs = (SelectQueryStruct) result;
    List<IQuerySelector> selectors = qs.getSelectors();
    assertNotNull(selectors);
    assertTrue(selectors.get(0) instanceof QueryColumnSelector);
  }

  @Test
  public void testSetQs() {
    // Execute
    reactor.setQs(selectQueryStruct);
  }

  @Test
  public void testGetName() {
    // Execute
    String name = reactor.getName();
    assertEquals("Delete", name);
  }
}
