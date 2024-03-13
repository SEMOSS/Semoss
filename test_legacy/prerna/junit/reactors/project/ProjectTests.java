package prerna.junit.reactors.project;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import prerna.project.impl.Project;

public class ProjectTests {

	@Test
	public void testProject() {
		Project p = new Project();
		assertNotNull(p);
	}
}
