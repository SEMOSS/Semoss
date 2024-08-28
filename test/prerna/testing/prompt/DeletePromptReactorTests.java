package prerna.testing.prompt;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;

public class DeletePromptReactorTests extends AbstractBaseSemossApiTests {
	
	@Test
	public void deletePromptTest() {
		String title = "Test-Title";
		String context = "Translate {{question}}";
		String intent = "Test Prompt";
		
		List<String> tags = Arrays.asList("World", "GAMING", "PLANTS"); 
		PromptTestUtils.addPrompt(title, context, intent, tags);

		NounMetadata listPrompts = PromptTestUtils.listPrompts();
		assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());
		
		List<Map<String, Object>> promptList = (List<Map<String, Object>>) listPrompts.getValue();
		String promptId = (String) promptList.get(0).get("ID");
		System.out.println(promptList);
		System.out.println(promptId);
		
		PromptTestUtils.deletePrompt(promptId);
		
		
		listPrompts = PromptTestUtils.listPrompts();
		assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());
	}
}
