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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import prerna.engine.api.IDatabaseEngine;
import prerna.util.Constants;

public class SesameJenaBooleanWrapperUnitTests {

  private SesameJenaBooleanWrapper wrapper;
  private IDatabaseEngine engine;

  @BeforeEach
  void setUp(@TempDir Path tempDir) throws Exception {
    wrapper = new SesameJenaBooleanWrapper();

    engine = new RDFFileSesameEngine();
    Path rdf = tempDir.resolve("rdf.owl");
    Files.createDirectories(rdf.getParent());
    URI uri = rdf.toUri();
    URL url = RdfUploadReactorUtility.class.getResource("movie-book-title.owl");
    String rdfPath = rdf.toAbsolutePath().toString();
    assert url != null;
    Path p = Paths.get(url.toURI());
    Files.copy(p, rdf);

    Path smss = tempDir.resolve("engine-01.smss");
    Files.createFile(smss);

    Properties props = new Properties();
    props.setProperty(Constants.ENGINE, "engine-01");
    props.setProperty(Constants.ENGINE_ALIAS, "ea");
    props.setProperty(Constants.RDF_FILE_NAME, rdfPath);
    props.setProperty(Constants.RDF_FILE_PATH, rdfPath);
    props.setProperty(Constants.RDF_FILE_BASE_URI, uri.toString());
    props.setProperty(Constants.RDF_FILE_TYPE, "RDF/XML");

    String typeQuery =
        "SELECT ?entity WHERE {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <@entity@>;}";
    props.setProperty(Constants.TYPE_QUERY, typeQuery);

    engine.setBasic(true);
    engine.open(props);

    wrapper.setEngine(engine);
  }

  @Test
  void testSesameJenaBooleanWrapper() throws Exception {
    String query =
        "ASK WHERE { \n"
            + "<http://semoss.org/ontologies/Relation/Contains/BOOK/TITLE> ?p ?o .\n"
            + "}";

    wrapper.setQuery(query);

    assertTrue(wrapper.execute());
  }
}
