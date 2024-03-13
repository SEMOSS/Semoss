package prerna.junit.reactors.comments;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import prerna.comments.InsightComment;

public class InsightCommentTests {
	
	@Test
	public void testInitialization() {
		InsightComment ic = new InsightComment("pid", "pname", "rdbmsId");
		assertNotNull(ic.getId());
	}

}
