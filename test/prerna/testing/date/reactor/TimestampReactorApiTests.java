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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import prerna.date.SemossDate;
import prerna.date.reactor.TimestampReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;

public class TimestampReactorApiTests extends AbstractBaseSemossApiTests {

	@Test
	public void getDefaultTimestamp() {
		String pixel = ApiSemossTestUtils.buildPixelCall(TimestampReactor.class, "date", null);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		SemossDate date = (SemossDate) nm.getValue();
		// Assuming the default format is "yyyy/MM/dd HH:mm:ss"
		String expectedPattern = "yyyy-MM-dd HH:mm:ss";
		assertEquals(expectedPattern, date.getPattern());
	}

	@Test
	public void getTimestampWithDateOnly() {
		String pixel = ApiSemossTestUtils.buildPixelCall(TimestampReactor.class, "date", "2022-03-19 01:20:12");
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		SemossDate date = (SemossDate) nm.getValue();
		String expectedDate = "2022-03-19 01:20:12";
		assertEquals(expectedDate, date.getFormattedDate());
	}

	@Test
	public void getTimestampWithDateAndFormat() {
		String pixel = ApiSemossTestUtils.buildPixelCall(TimestampReactor.class, "date", "2022/03/19 01:20:12",
				"format", "yyyy/MM/dd HH:mm:ss");
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		SemossDate date = (SemossDate) nm.getValue();
		String expectedDate = "2022/03/19 01:20:12";
		assertEquals(expectedDate, date.getFormattedDate());
	}
}
