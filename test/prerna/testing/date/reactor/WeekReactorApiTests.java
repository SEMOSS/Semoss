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
import prerna.date.SemossWeek;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;

public class WeekReactorApiTests extends AbstractBaseSemossApiTests {

	@Test
	public void getWeek() {
		String pixel = ApiSemossTestUtils.buildPixelCall("WEEK", "weeks", "52");

		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		SemossWeek week = (SemossWeek) nm.getValue();

		assertEquals(52, week.getNumWeeks());
	}
}
