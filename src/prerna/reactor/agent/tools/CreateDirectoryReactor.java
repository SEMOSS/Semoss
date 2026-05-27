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
 * Agent-facing alias for {@link prerna.reactor.insights.fs.NewInsightAssetsDirectoryReactor}.
 *
 * <p>Creates an empty directory in the insight assets folder. The parent reactor handles path
 * validation and directory creation; this subclass exists so the Pixel surface exposes the
 * familiar {@code CreateDirectory} name and an agent-tuned description.
 *
 * <p>{@link WriteFileReactor} already creates missing parent directories implicitly when
 * writing a file, so reach for this reactor only when you need an empty directory up front
 * (placeholder for git, target for a code generator, etc.).
 */
public class CreateDirectoryReactor extends prerna.reactor.insights.fs.NewInsightAssetsDirectoryReactor {

    @Override
    protected String getDescriptionForKey(String key) {
        if ("filePath".equals(key)) {
            return "Relative path of the directory to create inside the working directory.";
        }
        return super.getDescriptionForKey(key);
    }

    @Override
    public String getReactorDescription() {
        return "Creates an empty directory in the working directory. "
             + "WriteFile already creates missing parents implicitly, so use this only when "
             + "you need an empty directory up front.";
    }
}
