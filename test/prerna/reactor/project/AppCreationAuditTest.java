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
package prerna.reactor.project;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.project.impl.ProjectPortalsHelper;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.AssetUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;
import prerna.util.gson.GsonUtility;

class AppCreationAuditTest {
    @TempDir Path directory;

    @Test void blocksCreationEmitsOneSafeProjectEvent() throws Exception { blocks(false); }
    @Test void blocksWriteFailureDoesNotEmitSuccess() throws Exception { blocks(true); }

    private void blocks(boolean fail) throws Exception {
        User user = mock(User.class);
        Insight insight = mock(Insight.class);
        when(insight.getUser()).thenReturn(user);
        IProject project = mock(IProject.class);
        when(project.getProjectId()).thenReturn("new-project");
        when(project.getProjectName()).thenReturn("Test app");
        var reactor = new CreateAppFromBlocksReactor() {
            @Override protected void organizeKeys() {
                keyValue.put("project", "Test app");
                keyValue.put("global", "false");
            }
        };
        reactor.setInsight(insight);
        NounStore store = new NounStore("app-creation-test");
        store.makeNoun("json").add(new NounMetadata(Map.of("privateSource", "secret"), PixelDataType.MAP));
        reactor.setNounStore(store);
        try (var helper = mockStatic(ProjectHelper.class); var utility = mockStatic(Utility.class);
                var assets = mockStatic(AssetUtility.class); var json = mockStatic(GsonUtility.class);
                var git = mockStatic(GitRepoUtils.class); var cluster = mockStatic(ClusterUtil.class);
                var portals = mockStatic(ProjectPortalsHelper.class); var uploads = mockStatic(UploadUtilities.class);
                var audit = mockStatic(UserAuditTrailUtils.class)) {
            utility.when(() -> Utility.validateName(anyString())).thenReturn(true);
            helper.when(() -> ProjectHelper.generateNewProject(eq("Test app"), eq(IProject.PROJECT_TYPE.BLOCKS), eq(false),
                    isNull(), isNull(), eq(user), any(Logger.class))).thenReturn(project);
            assets.when(() -> AssetUtility.getProjectPortalsFolder("new-project")).thenReturn(directory.toString());
            assets.when(() -> AssetUtility.getProjectVersionFolder("Test app", "new-project")).thenReturn(directory.toString());
            uploads.when(() -> UploadUtilities.getProjectReturnData(user, "new-project")).thenReturn(Map.of());
            if (fail) {
                json.when(() -> GsonUtility.writeObjectToJsonFile(any(), any(), any())).thenThrow(new IOException("Cannot write blocks"));
                assertThrows(IllegalArgumentException.class, reactor::execute);
                audit.verifyNoInteractions();
            } else {
                reactor.execute();
                audit.verify(() -> UserAuditTrailUtils.recordProjectLifecycle(user, "PROJECT_CREATE", "new-project", "Test app",
                        Map.of("projectType", "BLOCKS", "global", false, "creationMethod", "BLOCKS")), times(1));
                audit.verifyNoMoreInteractions();
            }
        }
    }

    @Test void templateCloneAuditsTheNewProjectNotTheSource() {
        User user = mock(User.class);
        Insight insight = mock(Insight.class);
        when(insight.getUser()).thenReturn(user);
        IProject template = mock(IProject.class), project = mock(IProject.class);
        when(template.getProjectType()).thenReturn(IProject.PROJECT_TYPE.BLOCKS);
        when(template.getProjectName()).thenReturn("Template");
        when(project.getProjectId()).thenReturn("clone");
        var reactor = new CreateAppFromTemplateReactor() {
            @Override protected void organizeKeys() {
                keyValue.put("project", "Cloned app");
                keyValue.put("projectTemplate", "template");
            }
        };
        reactor.setInsight(insight);
        try (var helper = mockStatic(ProjectHelper.class); var utility = mockStatic(Utility.class);
                var security = mockStatic(SecurityProjectUtils.class); var assets = mockStatic(AssetUtility.class);
                var cluster = mockStatic(ClusterUtil.class); var uploads = mockStatic(UploadUtilities.class);
                var audit = mockStatic(UserAuditTrailUtils.class)) {
            security.when(() -> SecurityProjectUtils.userCanViewProject(user, "template")).thenReturn(true);
            security.when(() -> SecurityProjectUtils.userCanCloneProject(user, "template")).thenReturn(true);
            utility.when(() -> Utility.getProject("template")).thenReturn(template);
            helper.when(() -> ProjectHelper.generateNewProject(eq("Cloned app"), eq(IProject.PROJECT_TYPE.BLOCKS), eq(false),
                    isNull(), isNull(), eq(user), any(Logger.class))).thenReturn(project);
            assets.when(() -> AssetUtility.getProjectAssetsFolder(anyString())).thenReturn(directory.toString());
            assets.when(() -> AssetUtility.getProjectVersionFolder(anyString(), anyString())).thenReturn(directory.toString());
            uploads.when(() -> UploadUtilities.getProjectReturnData(user, "clone")).thenReturn(Map.of());
            reactor.execute();
            audit.verify(() -> UserAuditTrailUtils.recordProjectLifecycle(user, "PROJECT_CREATE", "clone", "Cloned app",
                    Map.of("projectType", "BLOCKS", "global", false, "creationMethod", "TEMPLATE")), times(1));
            audit.verifyNoMoreInteractions();
        }
    }
}
