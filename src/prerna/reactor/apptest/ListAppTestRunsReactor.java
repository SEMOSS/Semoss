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

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Bounded project-visible summaries; definitions and private evidence are not included. */
public class ListAppTestRunsReactor extends AbstractAppTestReactor {
    public ListAppTestRunsReactor() {
        keysToGet = new String[] { "project", "recordingId", "offset", "limit" };
        keyRequired = new int[] { 1, 0, 0, 0 };
    }

    @Override public NounMetadata execute() {
        organizeKeys();
        String project = targetProject();
        int offset = getInt("offset", 0), limit = getInt("limit", 20);
        if (offset < 0 || limit < 1 || limit > 50) throw new IllegalArgumentException("Use offset >= 0 and limit from 1 to 50");
        try {
            return new NounMetadata(AppTestHistory.list(AppTestHistory.refresh(project), project,
                    getString("recordingId"), offset, limit, Instant.now()), PixelDataType.MAP, PixelOperationType.OPERATION);
        } catch (IOException e) { throw new IllegalArgumentException("Unable to read app test run history", e); }
    }

    @Override public ReactorDefinition getReactorDefinition() {
        return super.getReactorDefinition().description("List persisted app test run summaries and latest per-test results. Requires app edit access. No test definitions, screenshots or captured page text. History retains the latest 100 finished runs per app plus active runs.")
                .key("project", "Required current app project id")
                .key("recordingId", "Optional test UUID to filter history without reading its definition")
                .key("offset", key -> key.type(Integer.class).description("Zero-based page offset; default 0"))
                .key("limit", key -> key.type(Integer.class).description("Page size from 1 to 50; default 20"))
                .output(Map.class, "Target, paginated run summaries, latest per-test results, total and hasMore");
    }
}
