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
package prerna.testing.query.querystruct.update.reactors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.reactors.UpdateReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;

public class UpdateReactorApiTests extends AbstractBaseSemossApiTests {

	@Test
	public void testCreateQueryStruct() {
		String pixel = ApiSemossTestUtils.buildPixelCall(UpdateReactor.class, "columns", "column1,column2", "values",
				"value1,value2");
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		UpdateQueryStruct qs = (UpdateQueryStruct) nm.getValue();

		assertEquals("column1,column2", (qs.getSelectors().get(0)).toString());

		assertEquals("value1,value2", qs.getValues().get(0), "The first update value should be value1");
	}
}
