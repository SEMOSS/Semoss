package prerna.junit.reactors.nameserver;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import prerna.nameserver.utility.MetamodelVertex;

public class MetamodelVertexTests {
	
	@Test
	public void testMetamodelVertex() {
		MetamodelVertex mv = new MetamodelVertex("test");
		assertEquals("test", mv.getConceptualName());
	}

}
