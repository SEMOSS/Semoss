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

/**
 * Agent-facing alias for {@link prerna.reactor.insights.fs.RenameInsightAssetReactor}.
 *
 * <p>Renames or moves a file/directory within the insight assets folder. The parent reactor
 * handles path validation and the rename operation; this subclass exists so the Pixel surface
 * exposes the familiar {@code MoveFile} name and an agent-tuned description.
 *
 * <p>Args remain {@code filePath} (source) and {@code newValue} (destination) — both interpreted
 * relative to the insight assets root.
 */
public class MoveFileReactor extends prerna.reactor.insights.fs.RenameInsightAssetReactor {

    @Override
    protected String getDescriptionForKey(String key) {
        if ("filePath".equals(key)) {
            return "Source file or directory to move (relative to the working directory).";
        } else if ("newValue".equals(key)) {
            return "Destination path for the move. Must not already exist; relative to the working directory.";
        }
        return super.getDescriptionForKey(key);
    }

    @Override
    public String getReactorDescription() {
        return "Moves or renames a file or directory inside the working directory. "
             + "Pass filePath (source) and newValue (destination). Use this for refactors and "
             + "reorganizations within the workspace.";
    }
}
