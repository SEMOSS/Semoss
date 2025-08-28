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
package prerna.reactor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import prerna.date.SemossDate;
import prerna.date.reactor.TimestampReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class TimestampReactorUnitTests {

	@InjectMocks
	private TimestampReactor reactor;

	@Mock
	private SemossDate mockDate;

	private Map<String, String> keyValues;

	@BeforeEach
	void setup() {
		MockitoAnnotations.openMocks(this);
		keyValues = new HashMap<>();
		reactor.keyValue = keyValues;
	}

	@Test
	void testDefaultTimestamp() {
		Calendar calendar = Calendar.getInstance();
		when(mockDate.getDate()).thenReturn(calendar.getTime());

		NounMetadata nm = reactor.execute();

		assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
		SemossDate date = (SemossDate) nm.getValue();
		assertEquals("yyyy-MM-dd HH:mm:ss", date.getPattern());
	}

	@Test
	void testTimestampWithDateOnly() {
		keyValues.put("date", "2022-03-19 01:20:12");

		NounMetadata nm = reactor.execute();

		assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
		SemossDate date = (SemossDate) nm.getValue();
		assertEquals("2022-03-19 01:20:12", date.getFormattedDate());
	}
}
