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
package prerna.testing.query.querystruct;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.Before;
import org.junit.Test;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.delete.DeleteReactor;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;

public class DeleteReactorApiTests {
  private DeleteReactor reactor;
  private NounStore nounStore;
  private SelectQueryStruct qs;
  private SelectQueryStruct exist_qs;
  private GenRowStruct mockGR;
  private IQuerySelector mockQueryS;

  @Before
  public void setUp() {
    reactor = new DeleteReactor();
    qs = new SelectQueryStruct();
    exist_qs = new SelectQueryStruct();
  }

  /*    @Test
  public void testMergeExistingValues(){
     //String engine = ApiSemossTestEngineUtils.createBasicEngine();
     String engine = ApiSemossTestEngineUtils.createBasicEngine();
  Map<String, Object> map = new HashMap<>();

     String pixel = ApiSemossTestUtils.buildPixelCall(DeleteReactor.class);
     NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
     TestEngineUtilities.setEngineMetadata(engine, map);

  }*/

  @Test
  public void testSetNounStore() {
    reactor.setNounStore(nounStore);
  }

  @Test
  public void testSetQs() {
    reactor.setQs(qs);
  }

  @Test
  public void testGetName() {
    String name = reactor.getName();
    assertEquals("Delete", name);
  }
}
