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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import java.util.Map;
import java.util.TreeSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AdminGetProjectAvailableReactorsReactorUnitTests {

    private AdminGetProjectAvailableReactorsReactor reactor;
    private Insight insight;
    private User user;

    private Map<String, String> keyValues;

    @BeforeEach
    void setup() {
        reactor = new AdminGetProjectAvailableReactorsReactor();
        insight = mock(Insight.class);
        user = mock(User.class);
        reactor.setInsight(insight);
        when(insight.getUser()).thenReturn(user);

        keyValues = reactor.keyValue;
    }

    @Test
    void testAdminUtilsNull() {
        try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
            sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);

            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
            assertEquals("User must be an admin to perform this function", e.getMessage());
        }
        //when(sau.apply(user)).thenReturn(null);
    }

    @Test
    void testProjectIdNull() throws Exception {
        try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {

            SecurityAdminUtils s = mock(SecurityAdminUtils.class);
            sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
            assertEquals("Must input an project id", e.getMessage());
        }
    }


    @Test
    void testProjectIdEmpty() {
        keyValues.put(ReactorKeysEnum.PROJECT.getKey(), "");
        try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {

            SecurityAdminUtils s = mock(SecurityAdminUtils.class);
            sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
            assertEquals("Must input an project id", e.getMessage());
        }
    }

    @Test
    void testExecuteNoReactorsReturned() {
        String projectAlias = "test";
        String projectId = "testy";
        keyValues.put(ReactorKeysEnum.PROJECT.getKey(), projectAlias);
        try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
                MockedStatic<SecurityProjectUtils> spu = Mockito.mockStatic(SecurityProjectUtils.class);
             MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
             ) {

            SecurityAdminUtils s = mock(SecurityAdminUtils.class);
            sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

            spu.when(() -> SecurityProjectUtils.testUserProjectIdForAlias(user, projectAlias))
                    .thenReturn(projectId);

            IProject project = mock(IProject.class);
            util.when(() -> Utility.getProject(projectId)).thenReturn(project);

            TreeSet<String> emptyTreeSet = new TreeSet<>();

            when(project.getAvailableReactors()).thenReturn(emptyTreeSet);
            NounMetadata nm = reactor.execute();
            assertNotNull(nm.getValue());
            TreeSet<String> retValue = (TreeSet<String>) nm.getValue();
            assertEquals(emptyTreeSet.size(), retValue.size());
            assertEquals(PixelDataType.CONST_STRING, nm.getNounType());

            spu.verify(() -> SecurityProjectUtils.testUserProjectIdForAlias(user, projectAlias), times(1));
            util.verify(() -> Utility.getProject(projectId), times(1));
        }
    }

    @Test
    void testExecuteOneReactorReturned() throws Exception {
        String projectAlias = "test";
        String projectId = "testy";
        keyValues.put(ReactorKeysEnum.PROJECT.getKey(), projectAlias);
        try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
             MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
             MockedStatic<SecurityProjectUtils> spu = Mockito.mockStatic(SecurityProjectUtils.class)) {

            SecurityAdminUtils s = mock(SecurityAdminUtils.class);
            sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);


            spu.when(() -> SecurityProjectUtils.testUserProjectIdForAlias(user, projectAlias))
                    .thenReturn(projectId);

            IProject project = mock(IProject.class);
            util.when(() -> Utility.getProject(projectId)).thenReturn(project);

            TreeSet<String> emptyTreeSet = new TreeSet<>();
            emptyTreeSet.add("reactor 1");

            when(project.getAvailableReactors()).thenReturn(emptyTreeSet);
            NounMetadata nm = reactor.execute();
            assertNotNull(nm.getValue());
            TreeSet<String> retValue = (TreeSet<String>) nm.getValue();
            assertEquals(emptyTreeSet.size(), retValue.size());
            assertEquals("reactor 1", retValue.first());
            assertEquals(PixelDataType.CONST_STRING, nm.getNounType());

            spu.verify(() -> SecurityProjectUtils.testUserProjectIdForAlias(user, projectAlias), times(1));
            util.verify(() -> Utility.getProject(projectId), times(1));
        }
    }
}
