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

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;

public class UpdatePromptReactorTests extends AbstractBaseSemossApiTests {

  @Test
  public void updateOnePromptTest() {
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

    title = "Updated Test Title";
    context = "Updated Context {{Location}}";
    intent = "Updated Test intent";
    PromptTestUtils.updatePrompt(promptId, title, context, intent, tags);

    listPrompts = PromptTestUtils.listPrompts();
    assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());

    title = "Updated Test Title2";
    context = "Updated Context {{Location}}2";
    intent = "Updated Test intent2";
    PromptTestUtils.updatePrompt(promptId, title, context, intent, tags);

    listPrompts = PromptTestUtils.listPrompts();
    assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());
  }

  @Test
  public void updateOnePromptTestTwo() {
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

    title = "Updated Test Title";
    context = "Updated Context {{Location}}";
    intent = "Updated Test intent";
    PromptTestUtils.updatePrompt(promptId, title, context, intent, null);

    listPrompts = PromptTestUtils.listPrompts();
    assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());

    title = "Updated Test Title2";
    context = "Updated Context {{Location}}2";
    intent = "Updated Test intent2";
    tags = Arrays.asList("Parth");
    PromptTestUtils.updatePrompt(promptId, title, context, intent, tags);

    listPrompts = PromptTestUtils.listPrompts();
    assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());
  }
}
