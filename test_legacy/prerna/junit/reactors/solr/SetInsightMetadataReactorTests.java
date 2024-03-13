package prerna.junit.reactors.solr;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import prerna.solr.reactor.SetInsightMetadataReactor;

public class SetInsightMetadataReactorTests {
	
	@Test
	public void testSetInsightMetadataReactorDescription() {
		SetInsightMetadataReactor reactor = new SetInsightMetadataReactor();
		assertEquals("Define metadata for an insight", reactor.getReactorDescription());
	}

}
