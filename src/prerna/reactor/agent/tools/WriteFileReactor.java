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
 * Agent-facing alias for {@link prerna.reactor.insights.fs.SaveInsightAssetsReactor}.
 *
 * <p>Writes (creates or overwrites) a file in the insight assets folder. The parent reactor
 * handles path validation, the actual write, and commits the change to the insight's git
 * repository. This subclass exists so the Pixel surface exposes the familiar {@code WriteFile}
 * name and an agent-tuned description.
 *
 * <p>Use {@link EditFileReactor} for targeted edits on existing files; use this when creating
 * a new file or doing a full rewrite.
 */
public class WriteFileReactor extends prerna.reactor.insights.fs.SaveInsightAssetsReactor {

    @Override
    public String getReactorDescription() {
        return "Writes a file in the working directory. Creates the file if missing, overwrites "
             + "if present, and commits the change to the insight repository. Use EditFile for "
             + "targeted edits to existing files.";
    }
}
