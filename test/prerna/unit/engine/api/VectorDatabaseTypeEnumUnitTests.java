package prerna.unit.engine.api;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import prerna.engine.api.VectorDatabaseTypeEnum;
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
		assertEquals(OpenSearchRestVectorDatabaseEngine.class.getName(), testEnum.getVectorDatabaseClass());
	}
	
	@Test
	void testElasticSearch() {
		VectorDatabaseTypeEnum testEnum = VectorDatabaseTypeEnum.ELASTIC_SEARCH;
		assertEquals("ELASTIC_SEARCH", testEnum.getVectorDatabaseName());
		assertEquals(ElasticSearchRestVectorDatabaseEngine.class.getName(), testEnum.getVectorDatabaseClass());
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
		try {
			VectorDatabaseTypeEnum.getEnumFromName(badName);
		} catch (Exception e) {
			assertEquals("Invalid input for name " + badName, e.getMessage());
		}
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
