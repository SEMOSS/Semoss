package prerna.testing.prompt;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import prerna.testing.AbstractBaseSemossApiTests;

public class CheckPromptTitleReactorTests extends AbstractBaseSemossApiTests {
	
	@Test
	public void titleExsitsTest() {
		String title = "Test-Title";
		String context = "Translate {{question}}";
		List<String> tags = Arrays.asList("World", "GAMING", "PLANTS"); 
		
		PromptTestUtils.addPrompt(title, context, tags, true, true);
		
		boolean titleExsits = PromptTestUtils.checkPromptTitle(title);
		assertTrue(titleExsits);
		
	}
	
	
	@Test
	public void titleDoesNotExsitsTest() {
		String title = "Test-Title";
		String context = "Translate {{question}}";
		List<String> tags = Arrays.asList("World", "GAMING", "PLANTS"); 
		
		PromptTestUtils.addPrompt(title, context, tags, true, true);
		
		// Changing vars for prompt 2 
		title = "Test-Title-2";

		boolean titleExsits = PromptTestUtils.checkPromptTitle(title);
		assertFalse(titleExsits);
		
	}

}
