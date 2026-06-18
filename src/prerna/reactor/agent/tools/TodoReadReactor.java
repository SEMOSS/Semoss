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
package prerna.reactor.agent.tools;

import java.io.File;
import java.nio.file.Files;
import org.json.JSONArray;
import org.json.JSONObject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reads the agent's current todo list from {@code todos.json} in the working directory.
 * Returns the full JSON document, or an empty-items document if the file does not yet exist.
 */
public class TodoReadReactor extends AbstractAgentToolReactor {

    private static final String TODOS_FILE = "todos.json";

    public TodoReadReactor() {
        this.keysToGet  = new String[]{};
        this.keyRequired = new int[]{};
    }

    @Override
    protected NounMetadata doExecute() throws Exception {
        File file = resolveAndValidate(TODOS_FILE);

        // not-yet-written is a normal state, not an error - return empty doc so the model
        // gets a useful signal ("no todos yet") instead of an error string
        if (!file.exists() || !file.isFile()) {
            JSONObject empty = new JSONObject();
            empty.put("updated_at", JSONObject.NULL);
            empty.put("items", new JSONArray());
            return new NounMetadata(empty.toString(2), PixelDataType.CONST_STRING);
        }

        String body = new String(Files.readAllBytes(file.toPath()));

        // light validation - if corrupt, surface raw content with a warning rather than crash
        try {
            new JSONObject(body);
        } catch (Exception e) {
            return new NounMetadata(
                    "Warning: todos.json is corrupt - raw content follows.\n\n" + body,
                    PixelDataType.CONST_STRING);
        }

        return new NounMetadata(body, PixelDataType.CONST_STRING);
    }

    @Override
    public String getReactorDescription() {
        return "Reads the current todo list from todos.json in the working directory. Returns "
             + "the full JSON document. Returns an empty list if no todos exist yet.";
    }
}
