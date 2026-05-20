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
 * Agent-facing alias for {@link prerna.reactor.insights.fs.BrowseInsightAssetsReactor}.
 *
 * <p>Lists the contents of a directory in the insight assets folder. The parent reactor handles
 * path validation and entry formatting; this subclass exists so the Pixel surface exposes the
 * familiar {@code ListDirectory} name and an agent-tuned description.
 */
public class ListDirectoryReactor extends prerna.reactor.insights.fs.BrowseInsightAssetsReactor {

    @Override
    public String getReactorDescription() {
        return "Lists files and directories at a relative path in the working directory. "
             + "Defaults to the working directory root when no path is given. Use this to "
             + "discover the shape of the workspace before reading or editing files.";
    }
}
