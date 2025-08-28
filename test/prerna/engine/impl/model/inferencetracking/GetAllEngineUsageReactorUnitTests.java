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
package prerna.engine.impl.model.inferencetracking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetAllEngineUsageReactorUnitTests {
	User user;
	Insight insight;
	Map<String, String> map;
	GetAllEngineUsageReactor reactor;

	@BeforeEach
	void setup() {
		map = new HashMap<>();
		map.put(ReactorKeysEnum.ENGINE.getKey(), "engine");
		map.put(ReactorKeysEnum.LIMIT.getKey(), "limit");
		map.put(ReactorKeysEnum.OFFSET.getKey(), "offset");
		map.put(ReactorKeysEnum.START_DATE.getKey(), "start date");
		map.put(ReactorKeysEnum.END_DATE.getKey(), "end date");

		user = mock(User.class);
		insight = mock(Insight.class);

		reactor = new GetAllEngineUsageReactor();

		reactor.setInsight(insight);
		when(insight.getUser()).thenReturn(user);
	}

	@Test
	void noEngineId() {
		map.remove(ReactorKeysEnum.ENGINE.getKey());
		reactor.keyValue = map;

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> reactor.execute());
		assertEquals("Must input an engine id", e.getMessage());
	}

	@Test
	void userNotOwner() {
		reactor.keyValue = map;

		try (MockedStatic<SecurityQueryUtils> queryUtils = Mockito.mockStatic(SecurityQueryUtils.class);
				MockedStatic<SecurityEngineUtils> engineUtils = Mockito.mockStatic(SecurityEngineUtils.class)) {
			queryUtils.when(() -> SecurityQueryUtils.testUserEngineIdForAlias(user, "engine")).thenReturn("engine");
			engineUtils.when(() -> SecurityEngineUtils.userIsOwner(user, "engine")).thenReturn(false);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> reactor.execute());
			assertEquals("Engine does not exist or user is not an owner of Engine", e.getMessage());
		}
	}

	@Test
	void allKeys() {
		List<Map<String, Object>> list = new ArrayList<>();

		reactor.keyValue = map;

		try (MockedStatic<SecurityQueryUtils> queryUtils = Mockito.mockStatic(SecurityQueryUtils.class);
				MockedStatic<SecurityEngineUtils> engineUtils = Mockito.mockStatic(SecurityEngineUtils.class);
				MockedStatic<ModelInferenceLogsUtils> modelUtils = Mockito.mockStatic(ModelInferenceLogsUtils.class)) {
			queryUtils.when(() -> SecurityQueryUtils.testUserEngineIdForAlias(user, "engine")).thenReturn("engine");
			engineUtils.when(() -> SecurityEngineUtils.userIsOwner(user, "engine")).thenReturn(true);
			modelUtils.when(() -> ModelInferenceLogsUtils.getOverAllEngineUsageFromModelInferenceLogs("engine", "limit",
					"offset", "start date", "end date")).thenReturn(list);

			NounMetadata n = reactor.execute();
			assertEquals(list, n.getValue());
			assertEquals(PixelDataType.FORMATTED_DATA_SET, n.getNounType());
		}
	}
}
