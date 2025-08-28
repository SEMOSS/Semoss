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
import prerna.engine.impl.model.BedrockEngine;
import prerna.engine.impl.model.EmbeddedModelEngine;
import prerna.engine.impl.model.KServeImageEmbedEngine;
import prerna.engine.impl.model.KServeVisionEngine;
import prerna.engine.impl.model.NEREngine;
import prerna.engine.impl.model.OpenAiEngine;
import prerna.engine.impl.model.TextEmbeddingsEngine;
import prerna.engine.impl.model.TextGenerationEngine;
import prerna.engine.impl.model.VertexEngine;
import prerna.engine.impl.remotesemoss.RemoteModelEngine;

public class ModelTypeEnumUnitTests {

	@Test
	void testBedrock() {
		ModelTypeEnum testEnum = ModelTypeEnum.BEDROCK;
		assertEquals("BEDROCK", testEnum.getModelName());
		assertEquals(BedrockEngine.class.getName(), testEnum.getModelClass());
	}

	@Test
	void testEmbedded() {
		ModelTypeEnum testEnum = ModelTypeEnum.EMBEDDED;
		assertEquals("EMBEDDED", testEnum.getModelName());
		assertEquals(EmbeddedModelEngine.class.getName(), testEnum.getModelClass());
	}

	@Test
	void testNER() {
		ModelTypeEnum testEnum = ModelTypeEnum.NER;
		assertEquals("NER", testEnum.getModelName());
		assertEquals(NEREngine.class.getName(), testEnum.getModelClass());
	}

	@Test
	void testVision() {
		ModelTypeEnum testEnum = ModelTypeEnum.KSERVE_VISION;
		assertEquals("KSERVE_VISION", testEnum.getModelName());
		assertEquals(KServeVisionEngine.class.getName(), testEnum.getModelClass());
	}

	@Test
	void testImageEmbed() {
		ModelTypeEnum testEnum = ModelTypeEnum.KSERVE_IMAGE_EMBED;
		assertEquals("KSERVE_IMAGE_EMBED", testEnum.getModelName());
		assertEquals(KServeImageEmbedEngine.class.getName(), testEnum.getModelClass());
	}

	@Test
	void testOpenAI() {
		ModelTypeEnum testEnum = ModelTypeEnum.OPEN_AI;
		assertEquals("OPEN_AI", testEnum.getModelName());
		assertEquals(OpenAiEngine.class.getName(), testEnum.getModelClass());
	}

	@Test
	void testRemote() {
		ModelTypeEnum testEnum = ModelTypeEnum.REMOTE;
		assertEquals("REMOTE", testEnum.getModelName());
		assertEquals(RemoteModelEngine.class.getName(), testEnum.getModelClass());
	}

	@Test
	void testTextEmbed() {
		ModelTypeEnum testEnum = ModelTypeEnum.TEXT_EMBEDDINGS;
		assertEquals("TEXT_EMBEDDINGS", testEnum.getModelName());
		assertEquals(TextEmbeddingsEngine.class.getName(), testEnum.getModelClass());
	}

	@Test
	void testTextGen() {
		ModelTypeEnum testEnum = ModelTypeEnum.TEXT_GENERATION;
		assertEquals("TEXT_GENERATION", testEnum.getModelName());
		assertEquals(TextGenerationEngine.class.getName(), testEnum.getModelClass());
	}

	@Test
	void testVertex() {
		ModelTypeEnum testEnum = ModelTypeEnum.VERTEX;
		assertEquals("VERTEX", testEnum.getModelName());
		assertEquals(VertexEngine.class.getName(), testEnum.getModelClass());
	}

	@Test
	void testBadFunctionName() {
		String badName = "NOT_A_REAL_MODEL_TYPE";
		Exception thrown = assertThrows(IllegalArgumentException.class, () -> ModelTypeEnum.getEnumFromName(badName));
		assertEquals("Invalid input for name " + badName, thrown.getMessage());
	}
}
