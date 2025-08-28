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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import prerna.engine.impl.function.AWSTextractFunctionEngine;
import prerna.engine.impl.function.AzureDocumentIntelligenceCustomEmbeddingsFuntionEngine;
import prerna.engine.impl.function.ImageDescriptionFunctionEngine;
import prerna.engine.impl.function.LocalPythonCustomEmbeddingsFunctionEngine;
import prerna.engine.impl.function.LocalPythonFunctionEngine;
import prerna.engine.impl.function.RESTFunctionEngine;

public class FunctionTypeEnumUnitTests {

  @Test
  void testAWSTextract() {
    FunctionTypeEnum testEnum = FunctionTypeEnum.AWS_TEXTRACT;
    assertEquals("AWS_TEXTRACT", testEnum.getFunctionName());
    assertEquals(AWSTextractFunctionEngine.class.getName(), testEnum.getFunctionClass());
  }

  @Test
  void testImageDescription() {
    FunctionTypeEnum testEnum = FunctionTypeEnum.IMAGE_DESCRIPTION;
    assertEquals("IMAGE_DESCRIPTION", testEnum.getFunctionName());
    assertEquals(ImageDescriptionFunctionEngine.class.getName(), testEnum.getFunctionClass());
  }

  @Test
  void testLocalPython() {
    FunctionTypeEnum testEnum = FunctionTypeEnum.LOCAL_PYTHON;
    assertEquals("LOCAL_PYTHON", testEnum.getFunctionName());
    assertEquals(LocalPythonFunctionEngine.class.getName(), testEnum.getFunctionClass());
  }

  @Test
  void testRest() {
    FunctionTypeEnum testEnum = FunctionTypeEnum.REST;
    assertEquals("REST", testEnum.getFunctionName());
    assertEquals(RESTFunctionEngine.class.getName(), testEnum.getFunctionClass());
  }

  @Test
  void testAzure() {
    FunctionTypeEnum testEnum = FunctionTypeEnum.AZURE_DOCUMENT_INTELLIGENCE_CUSTOM_EMBEDDINGS;
    assertEquals("AZURE_DOCUMENT_INTELLIGENCE_CUSTOM_EMBEDDINGS", testEnum.getFunctionName());
    assertEquals(
        AzureDocumentIntelligenceCustomEmbeddingsFuntionEngine.class.getName(),
        testEnum.getFunctionClass());
  }

  @Test
  void testCustomEmbeddings() {
    FunctionTypeEnum testEnum = FunctionTypeEnum.LOCAL_PYTHON_CUSTOM_EMBEDDINGS;
    assertEquals("LOCAL_PYTHON_CUSTOM_EMBEDDINGS", testEnum.getFunctionName());
    assertEquals(
        LocalPythonCustomEmbeddingsFunctionEngine.class.getName(), testEnum.getFunctionClass());
  }

  @Test
  void testBadFunctionName() {
    String badName = "NOT_A_REAL_FUNCTION";
    Exception thrown =
        assertThrows(
            IllegalArgumentException.class, () -> FunctionTypeEnum.getEnumFromName(badName));
    assertEquals("Invalid input for name " + badName, thrown.getMessage());
  }
}
