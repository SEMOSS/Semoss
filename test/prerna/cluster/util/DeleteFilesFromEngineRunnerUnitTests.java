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
package prerna.cluster.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.engine.api.IEngine;

class DeleteFilesFromEngineRunnerUnitTests {

    private static final String ENGINE_ID = "test-engine-id";
    private static final IEngine.CATALOG_TYPE ENGINE_TYPE = IEngine.CATALOG_TYPE.DATABASE;

    @Test
    void run_singleFile_callsDeleteOnce() {
        String[] filePaths = new String[] { "/path/to/file1.txt" };

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            DeleteFilesFromEngineRunner runner = new DeleteFilesFromEngineRunner(ENGINE_ID, ENGINE_TYPE, filePaths);
            runner.run();

            clusterUtilMock.verify(() ->
                ClusterUtil.deleteEngineCloudFile(ENGINE_ID, ENGINE_TYPE, "/path/to/file1.txt"),
                times(1)
            );
        }
    }

    @Test
    void run_multipleFiles_callsDeleteForEach() {
        String[] filePaths = new String[] {
            "/path/to/file1.txt",
            "/path/to/file2.csv",
            "/path/to/file3.json"
        };

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            DeleteFilesFromEngineRunner runner = new DeleteFilesFromEngineRunner(ENGINE_ID, ENGINE_TYPE, filePaths);
            runner.run();

            clusterUtilMock.verify(() ->
                ClusterUtil.deleteEngineCloudFile(eq(ENGINE_ID), eq(ENGINE_TYPE), anyString()),
                times(3)
            );
            clusterUtilMock.verify(() ->
                ClusterUtil.deleteEngineCloudFile(ENGINE_ID, ENGINE_TYPE, "/path/to/file1.txt")
            );
            clusterUtilMock.verify(() ->
                ClusterUtil.deleteEngineCloudFile(ENGINE_ID, ENGINE_TYPE, "/path/to/file2.csv")
            );
            clusterUtilMock.verify(() ->
                ClusterUtil.deleteEngineCloudFile(ENGINE_ID, ENGINE_TYPE, "/path/to/file3.json")
            );
        }
    }

    @Test
    void run_emptyArray_doesNotCallDelete() {
        String[] filePaths = new String[] {};

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            DeleteFilesFromEngineRunner runner = new DeleteFilesFromEngineRunner(ENGINE_ID, ENGINE_TYPE, filePaths);
            runner.run();

            clusterUtilMock.verify(() ->
                ClusterUtil.deleteEngineCloudFile(anyString(), any(IEngine.CATALOG_TYPE.class), anyString()),
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
                ClusterUtil.deleteEngineCloudFile(ENGINE_ID, ENGINE_TYPE, "/path/to/file2.csv")
            ).thenThrow(new RuntimeException("Delete failed for file2"));

            DeleteFilesFromEngineRunner runner = new DeleteFilesFromEngineRunner(ENGINE_ID, ENGINE_TYPE, filePaths);

            assertDoesNotThrow(() -> runner.run());

            clusterUtilMock.verify(() ->
                ClusterUtil.deleteEngineCloudFile(ENGINE_ID, ENGINE_TYPE, "/path/to/file1.txt")
            );
            clusterUtilMock.verify(() ->
                ClusterUtil.deleteEngineCloudFile(ENGINE_ID, ENGINE_TYPE, "/path/to/file2.csv")
            );
            clusterUtilMock.verify(() ->
                ClusterUtil.deleteEngineCloudFile(ENGINE_ID, ENGINE_TYPE, "/path/to/file3.json")
            );
        }
    }

    @Test
    void run_verifiesCorrectEngineIdAndTypePassedForEachFile() {
        String specificEngineId = "my-specific-engine-456";
        IEngine.CATALOG_TYPE specificType = IEngine.CATALOG_TYPE.STORAGE;
        String[] filePaths = new String[] { "/data/old-file.bin", "/data/temp.xml" };

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            DeleteFilesFromEngineRunner runner = new DeleteFilesFromEngineRunner(specificEngineId, specificType, filePaths);
            runner.run();

            clusterUtilMock.verify(() ->
                ClusterUtil.deleteEngineCloudFile(specificEngineId, specificType, "/data/old-file.bin")
            );
            clusterUtilMock.verify(() ->
                ClusterUtil.deleteEngineCloudFile(specificEngineId, specificType, "/data/temp.xml")
            );
            clusterUtilMock.verify(() ->
                ClusterUtil.deleteEngineCloudFile(eq(specificEngineId), eq(specificType), anyString()),
                times(2)
            );
        }
    }

    @Test
    void run_exceptionOnAllFiles_doesNotRethrow() {
        String[] filePaths = new String[] { "/fail1.txt", "/fail2.txt" };

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            clusterUtilMock.when(() ->
                ClusterUtil.deleteEngineCloudFile(eq(ENGINE_ID), eq(ENGINE_TYPE), anyString())
            ).thenThrow(new RuntimeException("Delete failed"));

            DeleteFilesFromEngineRunner runner = new DeleteFilesFromEngineRunner(ENGINE_ID, ENGINE_TYPE, filePaths);

            assertDoesNotThrow(() -> runner.run());

            clusterUtilMock.verify(() ->
                ClusterUtil.deleteEngineCloudFile(eq(ENGINE_ID), eq(ENGINE_TYPE), anyString()),
                times(2)
            );
        }
    }
}
