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

import java.util.Map;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Configure and inspect the platform-owned candidate/build/test/merge gate. */
public class ManageAppTestChangesReactor extends AbstractAppTestReactor {
    public ManageAppTestChangesReactor() {
        keysToGet=new String[]{"project","action","configuration"};keyRequired=new int[]{1,0,0};
    }
    @Override public NounMetadata execute() {
        organizeKeys();String project=targetProject();String action=getString("action");
        try {
            Object value=switch(action==null?"get":action) {
                case "get" -> AppChanges.get(project);
                case "testResult" -> AppChanges.testResult(project);
                case "buildLog" -> AppChanges.buildLog(project,AppTestRuns.ownerOf(requireUser()));
                case "save" -> AppChanges.save(project,getMap("configuration"));
                case "abandon" -> AppChanges.abandon(project,AppTestRuns.ownerOf(requireUser()));
                default -> throw new IllegalArgumentException("action must be get, save, testResult, buildLog or abandon");
            };
            return new NounMetadata(value,PixelDataType.MAP,PixelOperationType.OPERATION);
        }catch(RuntimeException failure){throw failure;}
        catch(Exception failure){throw new IllegalArgumentException("APP_CHANGE_FAILED: "+failure.getMessage(),failure);}
    }
    @Override public ReactorDefinition getReactorDefinition() {
        return super.getReactorDefinition()
            .description("Configure optional test-gated SEMOSS app chat changes. Enabled chats use a private branch from the configured base, build and run all saved workflows, and fast-forward only the exact passing candidate. Failed candidates remain for the same owner to repair in chat. Single-server SEMOSS harness only; existing base edits, conflicts and missing/changed regression coverage block merging. No GitHub push is performed.")
            .key("project","Required app project ID")
            .key("action","get (default), save, testResult (last completed candidate run, possibly before current edits; never a merge authorization), buildLog (owner-only private diagnostic), or abandon. Abandon preserves files/branch but makes the next chat start a new candidate.")
            .key("configuration",key->key.type(Map.class).description("For save: enabled boolean, baseBranch string (default master; select main when applicable), buildCommand string. Command runs in candidate assets with a two-minute limit; empty is allowed for static HTML apps without a build script. Requires installed build tools. Do not change workflow settings from a candidate chat."))
            .output(Map.class,"Target, configuration, active flag and latest change status/branch/commit/run IDs; private paths and build logs are excluded.");
    }
}
