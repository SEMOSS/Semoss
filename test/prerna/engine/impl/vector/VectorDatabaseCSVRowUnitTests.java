package prerna.engine.impl.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Vector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class VectorDatabaseCSVRowUnitTests {

	final private String source = "source";
	final private String modality = "modality";
	final private String divider = "divider";
	final private String part = "part";
	final private int tokens = 10;
	final private String content = "content";
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
