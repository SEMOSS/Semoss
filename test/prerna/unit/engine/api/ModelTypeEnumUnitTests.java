package prerna.unit.engine.api;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;

import prerna.engine.api.ModelTypeEnum;
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
		try {
			ModelTypeEnum.getEnumFromName(badName);
		} catch (Exception e) {
			assertEquals("Invalid input for name " + badName, e.getMessage());
		}
	}
}
