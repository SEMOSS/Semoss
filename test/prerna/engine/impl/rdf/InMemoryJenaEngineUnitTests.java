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
package prerna.engine.impl.rdf;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.update.UpdateAction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import prerna.engine.api.IDatabaseEngine;

public class InMemoryJenaEngineUnitTests {

  private InMemoryJenaEngine engine;

  @Mock private Model jenaModel;

  @BeforeEach
  public void setup() {
    MockitoAnnotations.openMocks(this);
    engine = new InMemoryJenaEngine();
    engine.jenaModel = jenaModel;
  }

  @Test
  void testExecQuery() {
    String queryString = "query";
    Query query = mock(Query.class);
    QueryExecution qex = mock(QueryExecution.class);
    ResultSet resultSet = mock(ResultSet.class);

    try (MockedStatic<QueryFactory> qf = Mockito.mockStatic(QueryFactory.class);
        MockedStatic<QueryExecutionFactory> qef =
            Mockito.mockStatic(QueryExecutionFactory.class); ) {

      qf.when(() -> QueryFactory.create(queryString)).thenReturn(query);
      qef.when(() -> QueryExecutionFactory.create(query, jenaModel)).thenReturn(qex);
      when(qex.execSelect()).thenReturn(resultSet);

      ResultSet results = (ResultSet) engine.execQuery("query");
      assertEquals(resultSet, results);
    }
  }

  @Test
  void testExecQueryErrorReturnsNull() {
    String queryString = "query";
    Query query = mock(Query.class);
    QueryExecution qex = mock(QueryExecution.class);

    try (MockedStatic<QueryFactory> qf = Mockito.mockStatic(QueryFactory.class);
        MockedStatic<QueryExecutionFactory> qef =
            Mockito.mockStatic(QueryExecutionFactory.class); ) {

      qf.when(() -> QueryFactory.create(queryString)).thenReturn(query);
      qef.when(() -> QueryExecutionFactory.create(query, jenaModel)).thenReturn(qex);

      RuntimeException e = new RuntimeException("test");
      when(qex.execSelect()).thenThrow(e);

      ResultSet results = (ResultSet) engine.execQuery("query");
      assertNull(results);
    }
  }

  @Test
  void testInsertData() {
    String queryString = "query";
    try (MockedStatic<UpdateAction> updateActionMock = Mockito.mockStatic(UpdateAction.class)) {
      engine.insertData(queryString);
      updateActionMock.verify(() -> UpdateAction.parseExecute(queryString, jenaModel), times(1));
    }
  }

  @Test
  void testSetModel() {
    Model jena2 = mock(Model.class);
    engine.setModel(jena2);
    assertEquals(jena2, engine.jenaModel);
  }

  @Test
  void testGetDatabaseType() {
    assertEquals(IDatabaseEngine.DATABASE_TYPE.JENA, engine.getDatabaseType());
  }

  @Test
  void testIsConnected() {
    assertFalse(engine.isConnected());
  }

  @Test
  void testHoldsFileLocks() {
    assertFalse(engine.holdsFileLocks());
  }
}
