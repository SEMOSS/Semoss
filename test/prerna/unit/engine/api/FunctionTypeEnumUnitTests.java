package prerna.unit.engine.api;

import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.impl.function.AWSTextractFunctionEngine;
import prerna.engine.impl.function.AzureDocumentIntelligenceCustomEmbeddingsFuntionEngine;
import prerna.engine.impl.function.ImageDescriptionFunctionEngine;
import prerna.engine.impl.function.LocalPythonCustomEmbeddingsFunctionEngine;
import prerna.engine.impl.function.LocalPythonFunctionEngine;
import prerna.engine.impl.function.RESTFunctionEngine;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;

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
		assertEquals(AzureDocumentIntelligenceCustomEmbeddingsFuntionEngine.class.getName(), testEnum.getFunctionClass());
	}
	
	@Test
	void testCustomEmbeddings() {
		FunctionTypeEnum testEnum = FunctionTypeEnum.LOCAL_PYTHON_CUSTOM_EMBEDDINGS;
		assertEquals("LOCAL_PYTHON_CUSTOM_EMBEDDINGS", testEnum.getFunctionName());
		assertEquals(LocalPythonCustomEmbeddingsFunctionEngine.class.getName(), testEnum.getFunctionClass());
	}
	
	@Test
	void testBadFunctionName() {
		String badName = "NOT_A_REAL_FUNCTION";
		try {
			FunctionTypeEnum.getEnumFromName(badName);
		} catch (Exception e) {
			assertEquals("Invalid input for name " + badName, e.getMessage());
		}
	}
}
