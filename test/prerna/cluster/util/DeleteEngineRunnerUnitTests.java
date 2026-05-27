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

class DeleteEngineRunnerUnitTests {

    @Test
    void run_callsDeleteEngineWithCorrectArguments() {
        String engineId = "engine-to-delete-123";
        IEngine.CATALOG_TYPE engineType = IEngine.CATALOG_TYPE.DATABASE;

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            DeleteEngineRunner runner = new DeleteEngineRunner(engineId, engineType);
            runner.run();

            clusterUtilMock.verify(() ->
                ClusterUtil.deleteEngine(engineId, engineType),
                times(1)
            );
        }
    }

    @Test
    void run_callsDeleteEngineWithStorageType() {
        String engineId = "storage-engine-456";
        IEngine.CATALOG_TYPE engineType = IEngine.CATALOG_TYPE.STORAGE;

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            DeleteEngineRunner runner = new DeleteEngineRunner(engineId, engineType);
            runner.run();

            clusterUtilMock.verify(() ->
                ClusterUtil.deleteEngine(engineId, engineType),
                times(1)
            );
        }
    }

    @Test
    void run_callsDeleteEngineWithModelType() {
        String engineId = "model-engine-789";
        IEngine.CATALOG_TYPE engineType = IEngine.CATALOG_TYPE.MODEL;

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            DeleteEngineRunner runner = new DeleteEngineRunner(engineId, engineType);
            runner.run();

            clusterUtilMock.verify(() ->
                ClusterUtil.deleteEngine(engineId, engineType),
                times(1)
            );
        }
    }

    @Test
    void run_exceptionIsCaughtSilently_doesNotRethrow() {
        String engineId = "failing-engine";
        IEngine.CATALOG_TYPE engineType = IEngine.CATALOG_TYPE.DATABASE;

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            clusterUtilMock.when(() ->
                ClusterUtil.deleteEngine(engineId, engineType)
            ).thenThrow(new RuntimeException("Delete failed"));

            DeleteEngineRunner runner = new DeleteEngineRunner(engineId, engineType);

            assertDoesNotThrow(() -> runner.run());
        }
    }

    @Test
    void run_exceptionIsCaughtSilently_illegalStateException() {
        String engineId = "bad-state-engine";
        IEngine.CATALOG_TYPE engineType = IEngine.CATALOG_TYPE.DATABASE;

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            clusterUtilMock.when(() ->
                ClusterUtil.deleteEngine(engineId, engineType)
            ).thenThrow(new IllegalStateException("Engine in bad state"));

            DeleteEngineRunner runner = new DeleteEngineRunner(engineId, engineType);

            assertDoesNotThrow(() -> runner.run());
        }
    }

    @Test
    void run_verifyDeleteEngineCalledExactlyOnce() {
        String engineId = "single-call-engine";
        IEngine.CATALOG_TYPE engineType = IEngine.CATALOG_TYPE.DATABASE;

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            DeleteEngineRunner runner = new DeleteEngineRunner(engineId, engineType);
            runner.run();

            clusterUtilMock.verify(() ->
                ClusterUtil.deleteEngine(anyString(), any(IEngine.CATALOG_TYPE.class)),
                times(1)
            );
        }
    }
}
