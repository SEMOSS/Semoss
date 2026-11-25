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
import java.sql.SQLException;
import java.util.Map;

import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Configure fixtures and open/close manual previews without changing the application's engine references. */
public class ManageAppTestDatabaseReactor extends AbstractAppTestReactor {
    public ManageAppTestDatabaseReactor() {
        this.keysToGet = new String[] { "project", "action", "configuration", "environmentId", "baselineCommitSha" };
        this.keyRequired = new int[] { 1, 0, 0, 0, 0 };
    }

    @Override
    @SuppressWarnings("unchecked")
    public NounMetadata execute() {
        organizeKeys();
        String target = targetProject();
        String action = getString("action");
        if (action == null || action.isBlank()) action = "get";
        try {
            Object result;
            switch (action) {
                case "get" -> result = Map.of("target", target, "configuration", AppTestEnvironment.configuration(target));
                case "save" -> result = Map.of("target", target, "configuration",
                        AppTestEnvironment.save(requireUser(), target, getMap("configuration")));
                case "approveBaseline" -> {
                    String sha = getString("baselineCommitSha");
                    if (sha == null) throw new IllegalArgumentException("Provide the Git SHA for the candidate baseline");
                    result = AppTestCandidate.approve(requireUser(), target, sha);
                }
                case "clearBaseline" -> result = AppTestCandidate.approve(requireUser(), target, null);
                case "open" -> {
                    var definition = AppTestEnvironment.definition(requireUser(), target);
                    if (definition == null) throw new IllegalArgumentException("Save and enable a test database before opening its preview");
                    var portal = AppTestEnvironment.portal(target);
                    result = AppTestEnvironment.open(AppTestRuns.ownerOf(requireUser()), target, definition, portal, true).started();
                }
                case "close" -> {
                    String id = AppTestRecordings.id(getString("environmentId"));
                    AppTestEnvironment.stop(AppTestRuns.ownerOf(requireUser()), target, id);
                    result = Map.of("target", target, "closed", true);
                }
                default -> throw new IllegalArgumentException("action must be get, save, open, close, approveBaseline or clearBaseline");
            }
            return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
        } catch (IOException | SQLException failure) {
            throw new IllegalArgumentException("TEST_DATABASE_SETUP_FAILED: " + failure.getMessage(), failure);
        }
    }

    @Override
    public ReactorDefinition getReactorDefinition() {
        return super.getReactorDefinition()
                .description("Manage an app's isolated H2 test database. Save source engine ID, schema SQL and synthetic seed SQL once; enabled configurations are used automatically by RunAppTests. Open creates a fresh manual recording preview; close destroys it. No source rows are copied and the source database is never queried or changed by these operations. Requires app edit and source engine view access.")
                .key("project", "Required target app project ID, never the toolbox ID")
                .key("action", "get (default), save, open, or close. App owners may approveBaseline or clearBaseline for candidate regressions.")
                .key("configuration", key -> key.type(Map.class).description("For save: enabled boolean, sourceEngineId string, schemaSql string with simple CREATE TABLE/INDEX, seedSql string with literal INSERTs (may be empty). Use synthetic data, never credentials or production rows. Disabled configurations use ordinary app tests."))
                .key("environmentId", "For close: the environmentId returned by open, belonging to this app and user")
                .key("baselineCommitSha", "For approveBaseline: full Git commit SHA containing the approved H2 fixture and saved tests")
                .output(Map.class, "Target and saved configuration, or temporary previewPath/environmentId/expiry. Preview URLs contain credentials: never save them in tests or repeat them in chat.");
    }
}
