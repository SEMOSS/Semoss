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
package prerna.project.impl.notebook;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

public class NotebookWriterFactory {

  /**
   * @param projectBlocksF
   * @return
   * @throws IOException
   */
  public static INotebookBuilder getNotebookBuilder(File projectBlocksF) throws IOException {
    JsonObject blocksFileJson = null;
    try (Reader fileReader = new FileReader(projectBlocksF)) {
      blocksFileJson = JsonParser.parseReader(fileReader).getAsJsonObject();
    }

    JsonElement versionBlock = blocksFileJson.get("version");
    String version = null;
    if (versionBlock != null) {
      version = versionBlock.getAsString();
    }

    INotebookBuilder builder = null;
    if (version == null) {
      builder = new prerna.project.impl.notebook.v1_0_0_alpha.NotebookWriter();
    } else {
      // only really have one, but this is to build out in the future
      builder = new prerna.project.impl.notebook.v1_0_0_alpha.NotebookWriter();
    }

    builder.setBlocksFileJson(blocksFileJson);

    return builder;
  }
}
