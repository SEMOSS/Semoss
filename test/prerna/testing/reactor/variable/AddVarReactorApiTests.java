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
package prerna.testing.reactor.variable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import prerna.reactor.AddVarReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;

public class AddVarReactorApiTests extends AbstractBaseSemossApiTests {

  // @Test
  public void testFullExecute() {
    // keys (language and format aren't required)
    String variable = "Test";
    String frame = "{ContractsPSCCombined1}"; //
    String expression = "x + y"; // expression that needs to be dynamically calculated
    String language = "r"; // R Python or Java
    String format = "jpeg"; // format to save as jpeg gif or png
    String pixel =
        ApiSemossTestUtils.buildPixelCall(
            AddVarReactor.class,
            ReactorKeysEnum.VARIABLE.getKey(),
            variable,
            ReactorKeysEnum.FRAME.getKey(),
            frame,
            ReactorKeysEnum.EXPRESSION.getKey(),
            expression,
            ReactorKeysEnum.LANGUAGE.getKey(),
            language,
            ReactorKeysEnum.FORMAT.getKey(),
            format);
    NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);

    assertNotNull(nm);
    assertEquals(PixelDataType.CONST_STRING, nm.getNounType());
  }

  public void testLangAndFormatNull() {}
}
