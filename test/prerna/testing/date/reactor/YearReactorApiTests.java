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
package prerna.testing.date.reactor;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;
import prerna.date.SemossYear;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;

public class YearReactorApiTests extends AbstractBaseSemossApiTests {

	@Test
	public void getYear() {
		String pixel = ApiSemossTestUtils.buildPixelCall("YEAR", "years", "2025");
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		SemossYear year = (SemossYear) nm.getValue();

		assertEquals(2025, year.getNumYears());
	}
}
