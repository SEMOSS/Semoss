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
package prerna.engine.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import prerna.engine.impl.vector.ChromaVectorDatabaseEngine;
import prerna.engine.impl.vector.ElasticSearchRestVectorDatabaseEngine;
import prerna.engine.impl.vector.FaissDatabaseEngine;
import prerna.engine.impl.vector.OpenSearchRestVectorDatabaseEngine;
import prerna.engine.impl.vector.PGVectorDatabaseEngine;
import prerna.engine.impl.vector.PineConeVectorDatabaseEngine;
import prerna.engine.impl.vector.WeaviateVectorDatabaseEngine;

public class VectorDatabaseTypeEnumUnitTests {

  @Test
  void testChroma() {
    VectorDatabaseTypeEnum testEnum = VectorDatabaseTypeEnum.CHROMA;
    assertEquals("CHROMA", testEnum.getVectorDatabaseName());
    assertEquals(ChromaVectorDatabaseEngine.class.getName(), testEnum.getVectorDatabaseClass());
  }

  @Test
  void testFAISS() {
    VectorDatabaseTypeEnum testEnum = VectorDatabaseTypeEnum.FAISS;
    assertEquals("FAISS", testEnum.getVectorDatabaseName());
    assertEquals(FaissDatabaseEngine.class.getName(), testEnum.getVectorDatabaseClass());
  }

  @Test
  void testPG() {
    VectorDatabaseTypeEnum testEnum = VectorDatabaseTypeEnum.PGVECTOR;
    assertEquals("PGVECTOR", testEnum.getVectorDatabaseName());
    assertEquals(PGVectorDatabaseEngine.class.getName(), testEnum.getVectorDatabaseClass());
  }

  @Test
  void testOpenSearch() {
    VectorDatabaseTypeEnum testEnum = VectorDatabaseTypeEnum.OPEN_SEARCH;
    assertEquals("OPEN_SEARCH", testEnum.getVectorDatabaseName());
    assertEquals(
        OpenSearchRestVectorDatabaseEngine.class.getName(), testEnum.getVectorDatabaseClass());
  }

  @Test
  void testElasticSearch() {
    VectorDatabaseTypeEnum testEnum = VectorDatabaseTypeEnum.ELASTIC_SEARCH;
    assertEquals("ELASTIC_SEARCH", testEnum.getVectorDatabaseName());
    assertEquals(
        ElasticSearchRestVectorDatabaseEngine.class.getName(), testEnum.getVectorDatabaseClass());
  }

  @Test
  void testWeaviate() {
    VectorDatabaseTypeEnum testEnum = VectorDatabaseTypeEnum.WEAVIATE;
    assertEquals("WEAVIATE", testEnum.getVectorDatabaseName());
    assertEquals(WeaviateVectorDatabaseEngine.class.getName(), testEnum.getVectorDatabaseClass());
  }

  @Test
  void testPineCone() {
    VectorDatabaseTypeEnum testEnum = VectorDatabaseTypeEnum.PINECONE;
    assertEquals("PINECONE", testEnum.getVectorDatabaseName());
    assertEquals(PineConeVectorDatabaseEngine.class.getName(), testEnum.getVectorDatabaseClass());
  }

  @Test
  void testBadVectorDatabaseName() {
    String badName = "NOT_A_REAL_DATABASE";
    Exception thrown =
        assertThrows(
            IllegalArgumentException.class, () -> VectorDatabaseTypeEnum.getEnumFromName(badName));
    assertEquals("Invalid input for name " + badName, thrown.getMessage());
  }

  @Test
  void testBadVectorDatabaseClass() {
    String badClass = "NOT_A_REAL_CLASS";
    VectorDatabaseTypeEnum badEnum = VectorDatabaseTypeEnum.getEnumFromClass(badClass);
    assertNull(badEnum);
  }

  @Test
  void testValidVectorDatabaseName() {
    String validName = "PINECONE";
    VectorDatabaseTypeEnum validEnum = VectorDatabaseTypeEnum.getEnumFromName(validName);
    assertNotNull(validEnum);
  }

  @Test
  void testValidVectorDatabaseClass() {
    String validClass = PineConeVectorDatabaseEngine.class.getName();
    VectorDatabaseTypeEnum validEnum = VectorDatabaseTypeEnum.getEnumFromClass(validClass);
    assertNotNull(validEnum);
  }
}
