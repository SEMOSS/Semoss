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
package prerna.reactor.agent.mcp.tools;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ListRoomFilesReactor extends AbstractReactor {

  @Override
  public NounMetadata execute() {
    Path roomPath = new File(insight.getInsightFolder()).toPath();

    List<String> fileList;
    try {
      fileList = RoomFileUtils.collectVisibleFiles(roomPath).stream()
          .map(p -> roomPath.relativize(p).toString())
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to list room files: " + e.getMessage());
    }

    return new NounMetadata(fileList, PixelDataType.MAP);
  }

  @Override
  public String getReactorDescription() {
    return "Lists all files in the room recursively, including files in subdirectories. "
        + "Paths are relative to the room folder. Hidden directories and files (starting with '.') "
        + "are always excluded.";
  }
}