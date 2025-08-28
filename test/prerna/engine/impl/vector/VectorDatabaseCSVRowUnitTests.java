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
package prerna.engine.impl.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Vector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class VectorDatabaseCSVRowUnitTests {

  private final String source = "source";
  private final String modality = "modality";
  private final String divider = "divider";
  private final String part = "part";
  private final int tokens = 10;
  private final String content = "content";
  private List<Double> embeddings;
  private VectorDatabaseCSVRow row;

  @BeforeEach
  void setUp() {
    embeddings = new Vector<>();
    embeddings.add(0.2);
    embeddings.add(0.4);
    embeddings.add(0.6);
    embeddings.add(0.8);
    embeddings.add(1.0);
    row = new VectorDatabaseCSVRow(source, modality, divider, part, tokens, content);
  }

  @Test
  void testGetSource() {
    assertEquals(source, row.getSource());
  }

  @Test
  void testGetModality() {
    assertEquals(modality, row.getModality());
  }

  @Test
  void testGetDivider() {
    assertEquals(divider, row.getDivider());
  }

  @Test
  void testGetPart() {
    assertEquals(part, row.getPart());
  }

  @Test
  void testGetTokens() {
    assertEquals(tokens, row.getTokens());
  }

  @Test
  void testGetContent() {
    assertEquals(content, row.getContent());
  }

  @Test
  void testEmbeddings() {
    row.setEmbeddings(embeddings);
    assertEquals(embeddings, row.getEmbeddings());
  }
}
