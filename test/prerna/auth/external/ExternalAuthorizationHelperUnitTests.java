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
package prerna.auth.external;

import org.junit.jupiter.api.Test;
import prerna.auth.User;

import static org.junit.jupiter.api.Assertions.*;

public class ExternalAuthorizationHelperUnitTests {

    @Test
    void invalidRecordDoesNotReturnPartialPermissions() throws Exception {
        try (org.mockito.MockedStatic<prerna.util.Utility> utility =
                org.mockito.Mockito.mockStatic(prerna.util.Utility.class);
             org.mockito.MockedStatic<prerna.util.BeanFiller> filler =
                org.mockito.Mockito.mockStatic(prerna.util.BeanFiller.class)) {
            utility.when(prerna.util.Utility::getBaseFolder).thenReturn(System.getProperty("java.io.tmpdir"));
            utility.when(() -> prerna.util.Utility.getDIHelperProperty(
                    prerna.util.Constants.EXTERNAL_PERMISSION_MANAGEMENT_ENGINEID)).thenReturn("id");
            utility.when(() -> prerna.util.Utility.getDIHelperProperty(
                    prerna.util.Constants.EXTERNAL_PERMISSION_MANAGEMENT_ENGINENAME)).thenReturn("name");
            utility.when(() -> prerna.util.Utility.getDIHelperProperty(
                    prerna.util.Constants.EXTERNAL_PERMISSION_MANAGEMENT_RESPONSE_JMES_PATH)).thenReturn("docs");
            com.fasterxml.jackson.databind.node.ArrayNode records =
                    new com.fasterxml.jackson.databind.ObjectMapper().createArrayNode();
            records.addObject().put("id", "valid-id").put("name", "Valid Model");
            filler.when(() -> prerna.util.BeanFiller.getJmesResult("response", "docs")).thenReturn(records);
            java.lang.reflect.Method transform = ExternalAuthorizationHelper.class.getDeclaredMethod(
                    "transformApiResponse", User.class, String.class);
            transform.setAccessible(true);
            assertEquals(1, ((java.util.List<?>) transform.invoke(null, new User(), "response")).size());
            records.addObject().put("id", "../outside").put("name", "Invalid Model");
            java.lang.reflect.InvocationTargetException error = assertThrows(
                    java.lang.reflect.InvocationTargetException.class,
                    () -> transform.invoke(null, new User(), "response"));
            assertInstanceOf(IllegalArgumentException.class, error.getCause());
        }
    }

    @Test
    void testUpdateException() {
        User u = new User();
        Exception e = assertThrows(Exception.class,
                () -> ExternalAuthorizationHelper.updateEnginePermissionsBasedOnApiCall(u));
        assertNotNull(e.getMessage());
    }
}
