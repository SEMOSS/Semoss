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

class DeleteProjectRunnerUnitTests {

    @Test
    void run_callsDeleteProjectWithCorrectProjectId() {
        String projectId = "project-to-delete-123";

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            DeleteProjectRunner runner = new DeleteProjectRunner(projectId);
            runner.run();

            clusterUtilMock.verify(() ->
                ClusterUtil.deleteProject(projectId),
                times(1)
            );
        }
    }

    @Test
    void run_callsDeleteProjectWithDifferentProjectId() {
        String projectId = "another-project-456";

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            DeleteProjectRunner runner = new DeleteProjectRunner(projectId);
            runner.run();

            clusterUtilMock.verify(() ->
                ClusterUtil.deleteProject(projectId),
                times(1)
            );
        }
    }

    @Test
    void run_exceptionIsCaughtSilently_doesNotRethrow() {
        String projectId = "failing-project";

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            clusterUtilMock.when(() ->
                ClusterUtil.deleteProject(projectId)
            ).thenThrow(new RuntimeException("Delete project failed"));

            DeleteProjectRunner runner = new DeleteProjectRunner(projectId);

            assertDoesNotThrow(() -> runner.run());
        }
    }

    @Test
    void run_exceptionIsCaughtSilently_illegalStateException() {
        String projectId = "bad-state-project";

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            clusterUtilMock.when(() ->
                ClusterUtil.deleteProject(projectId)
            ).thenThrow(new IllegalStateException("Project in bad state"));

            DeleteProjectRunner runner = new DeleteProjectRunner(projectId);

            assertDoesNotThrow(() -> runner.run());
        }
    }

    @Test
    void run_verifyDeleteProjectCalledExactlyOnce() {
        String projectId = "single-call-project";

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            DeleteProjectRunner runner = new DeleteProjectRunner(projectId);
            runner.run();

            clusterUtilMock.verify(() ->
                ClusterUtil.deleteProject(anyString()),
                times(1)
            );
        }
    }

    @Test
    void run_projectIdWithSpecialCharacters() {
        String projectId = "project-with-special_chars.v2";

        try (MockedStatic<ClusterUtil> clusterUtilMock = mockStatic(ClusterUtil.class)) {
            DeleteProjectRunner runner = new DeleteProjectRunner(projectId);
            runner.run();

            clusterUtilMock.verify(() ->
                ClusterUtil.deleteProject("project-with-special_chars.v2"),
                times(1)
            );
        }
    }
}
