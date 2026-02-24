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
/***********************************************************************
 * Copyright 2025 Semoss

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***********************************************************************/
package prerna.cluster.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.engine.api.IEngine;

class CopyFilesToEngineRunnerUnitTests {

    private static final String ENGINE_ID = "test-engine-id";
    private static final IEngine.CATALOG_TYPE ENGINE_TYPE = IEngine.CATALOG_TYPE.DATABASE;

    @Test
    void run_singleFile_callsCopyOnce() {
        String[] filePaths = new String[] { "/path/to/file1.txt" };

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            CopyFilesToEngineRunner runner = new CopyFilesToEngineRunner(ENGINE_ID, ENGINE_TYPE, filePaths);
            runner.run();

            clusterUtilMock.verify(() ->
                ClusterUtil.copyLocalFileToEngineCloudFolder(ENGINE_ID, ENGINE_TYPE, "/path/to/file1.txt"),
                times(1)
            );
        }
    }

    @Test
    void run_multipleFiles_callsCopyForEach() {
        String[] filePaths = new String[] {
            "/path/to/file1.txt",
            "/path/to/file2.csv",
            "/path/to/file3.json"
        };

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            CopyFilesToEngineRunner runner = new CopyFilesToEngineRunner(ENGINE_ID, ENGINE_TYPE, filePaths);
            runner.run();

            clusterUtilMock.verify(() ->
                ClusterUtil.copyLocalFileToEngineCloudFolder(eq(ENGINE_ID), eq(ENGINE_TYPE), anyString()),
                times(3)
            );
            clusterUtilMock.verify(() ->
                ClusterUtil.copyLocalFileToEngineCloudFolder(ENGINE_ID, ENGINE_TYPE, "/path/to/file1.txt")
            );
            clusterUtilMock.verify(() ->
                ClusterUtil.copyLocalFileToEngineCloudFolder(ENGINE_ID, ENGINE_TYPE, "/path/to/file2.csv")
            );
            clusterUtilMock.verify(() ->
                ClusterUtil.copyLocalFileToEngineCloudFolder(ENGINE_ID, ENGINE_TYPE, "/path/to/file3.json")
            );
        }
    }

    @Test
    void run_emptyArray_doesNotCallCopy() {
        String[] filePaths = new String[] {};

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            CopyFilesToEngineRunner runner = new CopyFilesToEngineRunner(ENGINE_ID, ENGINE_TYPE, filePaths);
            runner.run();

            clusterUtilMock.verify(() ->
                ClusterUtil.copyLocalFileToEngineCloudFolder(anyString(), any(IEngine.CATALOG_TYPE.class), anyString()),
                never()
            );
        }
    }

    @Test
    void run_exceptionOnOneFile_continuesProcessingRemainingFiles() {
        String[] filePaths = new String[] {
            "/path/to/file1.txt",
            "/path/to/file2.csv",
            "/path/to/file3.json"
        };

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            clusterUtilMock.when(() ->
                ClusterUtil.copyLocalFileToEngineCloudFolder(ENGINE_ID, ENGINE_TYPE, "/path/to/file1.txt")
            ).thenThrow(new RuntimeException("Copy failed for file1"));

            CopyFilesToEngineRunner runner = new CopyFilesToEngineRunner(ENGINE_ID, ENGINE_TYPE, filePaths);

            assertDoesNotThrow(() -> runner.run());

            clusterUtilMock.verify(() ->
                ClusterUtil.copyLocalFileToEngineCloudFolder(ENGINE_ID, ENGINE_TYPE, "/path/to/file1.txt")
            );
            clusterUtilMock.verify(() ->
                ClusterUtil.copyLocalFileToEngineCloudFolder(ENGINE_ID, ENGINE_TYPE, "/path/to/file2.csv")
            );
            clusterUtilMock.verify(() ->
                ClusterUtil.copyLocalFileToEngineCloudFolder(ENGINE_ID, ENGINE_TYPE, "/path/to/file3.json")
            );
        }
    }

    @Test
    void run_verifiesCorrectEngineIdAndTypePassedForEachFile() {
        String specificEngineId = "my-specific-engine-123";
        IEngine.CATALOG_TYPE specificType = IEngine.CATALOG_TYPE.STORAGE;
        String[] filePaths = new String[] { "/data/upload.bin", "/data/config.xml" };

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            CopyFilesToEngineRunner runner = new CopyFilesToEngineRunner(specificEngineId, specificType, filePaths);
            runner.run();

            clusterUtilMock.verify(() ->
                ClusterUtil.copyLocalFileToEngineCloudFolder(specificEngineId, specificType, "/data/upload.bin")
            );
            clusterUtilMock.verify(() ->
                ClusterUtil.copyLocalFileToEngineCloudFolder(specificEngineId, specificType, "/data/config.xml")
            );
            clusterUtilMock.verify(() ->
                ClusterUtil.copyLocalFileToEngineCloudFolder(eq(specificEngineId), eq(specificType), anyString()),
                times(2)
            );
        }
    }
}
