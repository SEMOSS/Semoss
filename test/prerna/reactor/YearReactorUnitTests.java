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

import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import prerna.date.SemossYear;
import prerna.date.reactor.YearReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class YearReactorUnitTests {

	private YearReactor reactor;
	private Map<String, String> keyValues;

	@BeforeEach
	void setup() {
		reactor = new YearReactor();
		keyValues = reactor.keyValue;
	}

	@Test
	void testYear() {
		keyValues.put("years", "2020");
		NounMetadata nm = reactor.execute();
		assertEquals(PixelDataType.CONST_YEAR, nm.getNounType());
		SemossYear sy = (SemossYear) nm.getValue();
		assertEquals(2020, sy.getNumYears());
	}
}
