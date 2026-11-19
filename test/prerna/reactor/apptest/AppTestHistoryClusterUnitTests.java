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
package prerna.reactor.apptest;

import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import prerna.cluster.util.clients.CentralCloudStorage;
import prerna.engine.api.IEngine;
import prerna.engine.impl.storage.AbstractStorageEngine;
import prerna.project.api.IProject;
import prerna.util.EngineSyncUtility;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.AssetUtility;
import prerna.cluster.util.ClusterUtil;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import java.util.Map;
import java.util.List;

/** Verify per-file project transfers never require a database-engine catalog lookup or folder sync. */
class AppTestHistoryClusterUnitTests {
    @TempDir Path temporary;

    @Test void publishesAndDeletesOnlyTheRunFileUsingTheProjectCatalog() throws Exception {
        CentralCloudStorage client = mock(CentralCloudStorage.class, CALLS_REAL_METHODS);
        AbstractStorageEngine storage = mock(AbstractStorageEngine.class);
        Field field = CentralCloudStorage.class.getDeclaredField("centralStorageEngine");
        field.setAccessible(true);
        Object previous = field.get(null);
        field.set(null, storage);
        IProject project = mock(IProject.class);
        when(project.getEngineName()).thenReturn("Example");
        String id = "app-history";
        Path file = temporary.resolve("Example__app-history/app_root/version/assets/app-tests/test-runs/run.json");
        Files.createDirectories(file.getParent());
        Files.writeString(file, "{}");
        try (MockedStatic<Utility> utility = mockStatic(Utility.class);
                MockedStatic<EngineUtility> engineUtility = mockStatic(EngineUtility.class);
                MockedStatic<EngineSyncUtility> locks = mockStatic(EngineSyncUtility.class)) {
            utility.when(() -> Utility.getProject(id, false)).thenReturn(project);
            engineUtility.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.PROJECT)).thenReturn(temporary.toString());
            locks.when(() -> EngineSyncUtility.getEngineLock(id)).thenReturn(new ReentrantLock());
            client.copyLocalFileToEngineCloudFolder(id, IEngine.CATALOG_TYPE.PROJECT, file.toString());
            verify(storage).copyToStorage(eq(file.toString()), contains("app-tests/test-runs"), isNull());
            client.deleteEngineCloudFile(id, IEngine.CATALOG_TYPE.PROJECT, file.toString());
            verify(storage).deleteFromStorage(contains("app-tests/test-runs/run.json"));
            verify(storage, never()).syncLocalToStorage(anyString(), anyString(), any());
        } finally { field.set(null, previous); }
    }

    @Test void savingAndDeletingDefinitionsSynchronizesOnlyTestsAndKeepsHistory() throws Exception {
        Path base = temporary.resolve("assets/app-tests");
        Files.createDirectories(base.resolve("test-runs"));
        Path evidence = base.resolve("test-runs/kept.json");
        Files.writeString(evidence, "existing history");
        IProject project = mock(IProject.class);
        List<String> published = new java.util.ArrayList<>();
        try (MockedStatic<Utility> utility = mockStatic(Utility.class);
                MockedStatic<AssetUtility> assets = mockStatic(AssetUtility.class);
                MockedStatic<ClusterUtil> cluster = mockStatic(ClusterUtil.class)) {
            utility.when(() -> Utility.getProject("app")).thenReturn(project);
            assets.when(() -> AssetUtility.getProjectAssetsFolder("app")).thenReturn(base.getParent().toString());
            assets.when(() -> AssetUtility.getProjectAppRootFolder("app")).thenReturn(temporary.resolve("private").toString());
            cluster.when(() -> ClusterUtil.pushProjectFolder(eq(project), anyString())).thenAnswer(invocation -> {
                published.add(invocation.getArgument(1));
                return null;
            });
            SaveAppTestRecordingReactor save = new SaveAppTestRecordingReactor() {
                @Override protected String targetProject() { return "app"; }
            };
            save.getNounStore().makeGenRowStruct("project").addLiteral("app");
            save.getNounStore().makeGenRowStruct("recording").add(new NounMetadata(Map.of("name", "Example", "description", "Check a visible result.",
                    "steps", List.of(Map.of("type", "ASSERT_VISIBLE", "selector", "#result"))), PixelDataType.MAP));
            Map<?, ?> saved = (Map<?, ?>) save.execute().getValue();
            DeleteAppTestRecordingReactor remove = new DeleteAppTestRecordingReactor() {
                @Override protected String targetProject() { return "app"; }
            };
            remove.getNounStore().makeGenRowStruct("project").addLiteral("app");
            remove.getNounStore().makeGenRowStruct("recordingId").addLiteral(String.valueOf(saved.get("id")));
            remove.execute();
            org.junit.jupiter.api.Assertions.assertEquals(List.of(base.resolve("tests").toString(), base.resolve("tests").toString()), published);
            org.junit.jupiter.api.Assertions.assertEquals("existing history", Files.readString(evidence));
        }
    }
}
