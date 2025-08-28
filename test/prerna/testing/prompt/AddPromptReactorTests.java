/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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
    String intent = "Test Prompt";

    List<String> tags = Arrays.asList("World", "GAMING", "PLANTS");
    PromptTestUtils.addPrompt(title, context, intent, tags);

    NounMetadata listPrompts = PromptTestUtils.listPrompts();
    assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());
  }

  @Test
  public void addTwoPrompts() {

    String title = "Test-Title";
    String context = "Translate {{question}}";
    String intent = "Test Prompt";
    List<String> tags = Arrays.asList("World", "GAMING", "PLANTS");

    PromptTestUtils.addPrompt(title, context, intent, tags);

    // Changing vars for prompt 2
    title = "Test-Title-2";
    context = "Translate the {{question}} int {{language}}";
    tags = Arrays.asList("World", "Travel");
    intent = "Test Prompt Intent 2";
    PromptTestUtils.addPrompt(title, context, intent, tags);

    NounMetadata listPrompts = PromptTestUtils.listPrompts();
    assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());
  }

  @Test
  public void addTwoPromptsGetOneTag() {

    String title = "Test-Title";
    String context = "Translate {{question}}";
    String intent = "Test Prompt";
    List<String> tags = Arrays.asList("World", "GAMING", "PLANTS");

    PromptTestUtils.addPrompt(title, context, intent, tags);

    // Changing vars for prompt 2
    title = "Test-Title-2";
    context = "Translate the {{question}} int {{language}}";
    tags = Arrays.asList("World", "Travel");
    intent = "second intent";
    PromptTestUtils.addPrompt(title, context, intent, tags);

    List<String> metaTagsFilters = Arrays.asList("World");
    NounMetadata listPrompts = PromptTestUtils.listPrompts(metaTagsFilters);
    assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());
  }

  @Test
  public void addPromptValidationTest() {
    String title = "Test-Title";
    String context = "Translate {{question}}";
    String intent = "Test Prompt";
    List<String> tags = Arrays.asList("World", "GAMING", "PLANTS");
    PromptTestUtils.addPrompt(title, context, intent, tags);

    NounMetadata listPrompts = PromptTestUtils.listPrompts();
    assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());
  }
}
