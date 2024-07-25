package prerna.testing.prompt;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;

public class AddPromptReactorTests extends AbstractBaseSemossApiTests {
	
	@Test
	public void addOnePromptTest() {
		String title = "Test-Title";
		String context = "Translate {{question}}";
		List<String> tags = Arrays.asList("World", "GAMING", "PLANTS"); 
		PromptTestUtils.addPrompt(title, context, tags, true, true);

		NounMetadata listPrompts = PromptTestUtils.listPrompts();
		assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());
	}
	
	@Test
	public void addTwoPrompts() {
			
		String title = "Test-Title";
		String context = "Translate {{question}}";
		List<String> tags = Arrays.asList("World", "GAMING", "PLANTS"); 
		
		PromptTestUtils.addPrompt(title, context, tags, true, true);
		
		// Changing vars for prompt 2 
		title = "Test-Title-2";
		context = "Translate the {{question}} int {{language}}";
		tags = Arrays.asList("World", "Travel");
		PromptTestUtils.addPrompt(title, context, tags, true, true);
		
		NounMetadata listPrompts = PromptTestUtils.listPrompts();
		assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());
		
	}
	
	@Test
	public void addTwoPromptsGetOneTag() {
			
		String title = "Test-Title";
		String context = "Translate {{question}}";
		List<String> tags = Arrays.asList("World", "GAMING", "PLANTS"); 
		
		PromptTestUtils.addPrompt(title, context, tags, true, true);
		
		// Changing vars for prompt 2 
		title = "Test-Title-2";
		context = "Translate the {{question}} int {{language}}";
		tags = Arrays.asList("World", "Travel");
		PromptTestUtils.addPrompt(title, context, tags, true, true);
		
		List<String> metaTagsFilters = Arrays.asList("World");
		NounMetadata listPrompts = PromptTestUtils.listPrompts(metaTagsFilters);
		assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());
		
	}
	
	@Test
	public void addPromptValidationTest() {
		String title = "Test-Title";
		String context = "Translate {{question}}";
		List<String> tags = Arrays.asList("World", "GAMING", "PLANTS"); 
		PromptTestUtils.addPrompt(title, context, tags, true, true);

		NounMetadata listPrompts = PromptTestUtils.listPrompts();
		assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());
	}

}
