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
package prerna.auth.utils.reactors.admin;

import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;
import java.util.ArrayList;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminGetAllEngineUsageReactorUnitTests {
	private User user;
	private Insight insight;
	private AdminGetAllEngineUsageReactor reactor;
	private Map<String, String> keyValues;

	@BeforeEach
    void setUp() {
        reactor = new AdminGetAllEngineUsageReactor();
		keyValues = reactor.keyValue;
		insight = mock(Insight.class);
        user = mock(User.class);
		
        reactor.setInsight(insight);

        when(insight.getUser()).thenReturn(user);
    }

	@Test
	public void notAdmin() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("User must be an admin to perform this function", e.getMessage());
		}
	}

	@Test
	public void noEngine() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Must input an engine id", e.getMessage());
		}
	}

	@Test
	public void test() {
		keyValues.put(ReactorKeysEnum.ENGINE.getKey(), "engine");
		keyValues.put(ReactorKeysEnum.LIMIT.getKey(), "limit");
		keyValues.put(ReactorKeysEnum.OFFSET.getKey(), "offset");

        try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
            MockedStatic<SecurityQueryUtils> squ = Mockito.mockStatic(SecurityQueryUtils.class);
            MockedStatic<ModelInferenceLogsUtils> modelInference = Mockito.mockStatic(ModelInferenceLogsUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
                sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
                squ.when(() -> SecurityQueryUtils.testUserEngineIdForAlias(user, "engine")).thenReturn("engine");
                modelInference.when(() -> 
                        ModelInferenceLogsUtils.getOverAllEngineUsageFromModelInferenceLogs("engine", "limit", "offset", ReactorKeysEnum.START_DATE.getKey(), ReactorKeysEnum.END_DATE.getKey())
                    ).thenReturn(new ArrayList<Map<String, Object>>());

                NounMetadata nm = reactor.execute();

                assertNotNull(nm);
                assertEquals(PixelDataType.FORMATTED_DATA_SET, nm.getNounType());
                assertEquals(new ArrayList<Map<String, Object>>(), nm.getValue());
		}
	}
}
