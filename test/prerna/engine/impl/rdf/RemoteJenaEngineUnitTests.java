/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.impl.rdf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.exec.http.QueryExecutionHTTP;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import prerna.engine.api.IDatabaseEngine;
import prerna.util.Constants;

public class RemoteJenaEngineUnitTests {

  private RemoteJenaEngine engine;
  @Mock private Model jenaModel;

  @BeforeEach
  public void setUp(@TempDir Path tempDir) throws Exception {
    MockitoAnnotations.openMocks(this);
    engine = new RemoteJenaEngine();
    Path rdf = tempDir.resolve("rdf.owl");
    Files.createDirectories(rdf.getParent());
    URI uri = rdf.toUri();
    URL url = RdfUploadReactorUtility.class.getResource("movie-book-title.owl");
    assert url != null;
    Path p = Paths.get(url.toURI());
    Files.copy(p, rdf);

    Path smss = tempDir.resolve("engine-01.smss");
    Files.createFile(smss);

    Properties props = new Properties();
    props.setProperty(Constants.ENGINE, "engine-01");
    props.setProperty(Constants.ENGINE_ALIAS, "ea");
    props.setProperty(Constants.SPARQL_QUERY_ENDPOINT, "semoss.org");
    props.setProperty(Constants.URL_PARAM, "one;two");
    props.setProperty("one", "test1");
    props.setProperty("two", "test2");

    // not exactly sure what to put here
    String typeQuery = "";
    props.setProperty(Constants.TYPE_QUERY, "");

    engine.setSmssProp(props);

    engine.setBasic(true);

    engine.jenaModel = jenaModel;

    engine.open(smss.toAbsolutePath().toString());
  }

  @Test
  void testOpen() {
    assertTrue(engine.isConnected());
    assertNotNull(engine.serviceURI);
  }

  @Test
  void testClose() throws IOException {
    engine.close();

    verify(jenaModel, times(1)).close();
  }

  @Test
  void testExecQuery() {
    String query =
        "ASK WHERE { \n"
            + "<http://semoss.org/ontologies/Relation/Contains/BOOK/TITLE> ?p ?o .\n"
            + "}";

    String expectedFinalUrl = "semoss.org?one=test1&two=test2";

    try (MockedStatic<QueryExecutionFactory> factory =
        Mockito.mockStatic(QueryExecutionFactory.class)) {
      QueryExecution qexec = mock(QueryExecution.class);
      factory
          .when(() -> QueryExecutionHTTP.service(expectedFinalUrl).query(query).build())
          .thenReturn(qexec);

      when(qexec.execAsk()).thenReturn(true);
      Boolean bool = (Boolean) engine.execQuery(query);
      assertTrue(bool);
    }
  }

  // @Test
  // need to investigate real world use cases
  void testExecQueryConstruct() {
    String query =
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
            + "PREFIX semoss: <http://semoss.org/ontologies/>\n"
            + "\n"
            + "CONSTRUCT {\n"
            + "  ?title rdf:type semoss:Concept .\n"
            + "  ?title semoss:Relation/Contains ?category .\n"
            + "} \n"
            + "WHERE {\n"
            + "  ?title rdf:type semoss:Concept .\n"
            + "  ?title semoss:Relation/Contains ?category .\n"
            + "  FILTER (?category IN (semoss:Relation/Contains/MOVIE/TITLE, semoss:Relation/Contains/BOOK/TITLE))\n"
            + "}";

    String expectedFinalUrl = "semoss.org?one=test1&two=test2";

    try (MockedStatic<QueryExecutionFactory> factory =
        Mockito.mockStatic(QueryExecutionFactory.class)) {
      QueryExecution qexec = mock(QueryExecution.class);
      factory
          .when(() -> QueryExecutionHTTP.service(expectedFinalUrl).query(query).build())
          .thenReturn(qexec);

      Model model = mock(Model.class);
      when(qexec.execConstruct()).thenReturn(model);
      Model result = (Model) engine.execQuery(query);
      assertEquals(model, result);
    }
  }

  // @Test
  // Need to investigate real world use cases
  void testExecQuerySelect() {
    String query =
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
            + "PREFIX semoss: <http://semoss.org/ontologies/>\n"
            + "\n"
            + "SELECT ?title ?category\n"
            + "WHERE {\n"
            + "  ?title rdf:type semoss:Concept .\n"
            + "  ?title semoss:Relation/Contains ?category .\n"
            + "  FILTER (?category IN (semoss:Relation/Contains/MOVIE/TITLE, semoss:Relation/Contains/BOOK/TITLE))\n"
            + "}";

    String expectedFinalUrl = "semoss.org?one=test1&two=test2";

    try (MockedStatic<QueryExecutionFactory> factory =
        Mockito.mockStatic(QueryExecutionFactory.class)) {
      QueryExecution qexec = mock(QueryExecution.class);
      factory
          .when(() -> QueryExecutionHTTP.service(expectedFinalUrl).query(query).build())
          .thenReturn(qexec);

      ResultSet rs = mock(ResultSet.class);
      when(qexec.execSelect()).thenReturn(rs);
      ResultSet result = (ResultSet) engine.execQuery(query);
      assertEquals(rs, result);
    }
  }

  @Test
  void testDatabaseType() {
    assertEquals(IDatabaseEngine.DATABASE_TYPE.JENA, engine.getDatabaseType());
  }

  @Test
  void testIsConnected() {
    assertTrue(engine.isConnected());
  }

  @Test
  void testDelete() {
    engine.delete();
    verifyNoInteractions(jenaModel);
  }

  @Test
  void testHoldsFileLocks() {
    assertFalse(engine.holdsFileLocks());
  }
}
