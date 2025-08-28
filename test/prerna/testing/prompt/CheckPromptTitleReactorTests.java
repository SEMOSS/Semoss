/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
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
		String intent = "Test Prompt";
		List<String> tags = Arrays.asList("World", "GAMING", "PLANTS");

		PromptTestUtils.addPrompt(title, context, intent, tags);

		boolean titleExsits = PromptTestUtils.checkPromptTitle(title);
		assertTrue(titleExsits);
	}

	@Test
	public void titleDoesNotExsitsTest() {
		String title = "Test-Title";
		String context = "Translate {{question}}";
		List<String> tags = Arrays.asList("World", "GAMING", "PLANTS");
		String intent = "Test Prompt";

		PromptTestUtils.addPrompt(title, context, intent, tags);

		// Changing vars for prompt 2
		title = "Test-Title-2";

		boolean titleExsits = PromptTestUtils.checkPromptTitle(title);
		assertFalse(titleExsits);
	}
}
