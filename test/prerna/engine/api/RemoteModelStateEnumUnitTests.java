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
package prerna.engine.api;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RemoteModelStateEnumUnitTests {
	private RemoteModelStateEnum[] enumArr;

	@BeforeEach
	void setUp() {
		enumArr = RemoteModelStateEnum.values();
	}

	@Test
	void testAllEnumsExist() {
		List<RemoteModelStateEnum> enumLst = Stream
				.of(RemoteModelStateEnum.COLD, RemoteModelStateEnum.WARMING, RemoteModelStateEnum.ACTIVE,
						RemoteModelStateEnum.FAILED, RemoteModelStateEnum.UNKNOWN)
				.collect(Collectors.toCollection(Vector::new));

		assertTrue(CollectionUtils.containsAll(enumLst, Arrays.asList(enumArr)));
	}
}
