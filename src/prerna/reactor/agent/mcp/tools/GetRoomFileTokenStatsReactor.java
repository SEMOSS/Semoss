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

import prerna.util.files.SemossParsedFile;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Returns approximate token statistics for extracted room files. */
public class GetRoomFileTokenStatsReactor extends AbstractReactor {

  public GetRoomFileTokenStatsReactor() {
    this.keysToGet = new String[] {};
    this.keyRequired = new int[] {};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    Path roomPath = new File(insight.getInsightFolder()).toPath();

    List<Path> filePaths;
    try {
      filePaths = RoomFileUtils.collectVisibleFiles(roomPath);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to list room files: " + e.getMessage());
    }

    Map<String, Map<String, Object>> results = filePaths.parallelStream()
        .map(path -> {
          File file = path.toFile();
          String relativePath = roomPath.relativize(path).toString();
          Map<String, Object> fileResult = new HashMap<>();
          try {
            SemossParsedFile semossParsedFile = new SemossParsedFile(file);
            String extractedContent = semossParsedFile.getExtractedContents();
            if (extractedContent == null) {
              fileResult.put("status", "no_content");
            } else {
              int charCount = extractedContent.length();
              int wordCount = countWords(extractedContent);
              int approxTokens = (int) Math.ceil(charCount / 4.0);
              fileResult.put("status", "ok");
              fileResult.put("extractedPath", semossParsedFile.getExtractedContentsFilePath());
              fileResult.put("charCount", charCount);
              fileResult.put("wordCount", wordCount);
              fileResult.put("approxTokens", approxTokens);
            }
          } catch (IOException e) {
            fileResult.put("status", "error");
            fileResult.put("message", e.getMessage());
          }
          return new AbstractMap.SimpleEntry<>(relativePath, fileResult);
        })
        .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));

    return new NounMetadata(results, PixelDataType.MAP);
  }

  @Override
  public String getReactorDescription() {
    return "Returns approximate token counts for extracted room files (char/word counts +"
        + " approxTokens).";
  }

  private int countWords(String content) {
    int count = 0;
    boolean inWord = false;
    for (int i = 0; i < content.length(); i++) {
      char c = content.charAt(i);
      if (Character.isWhitespace(c)) {
        inWord = false;
      } else if (!inWord) {
        count++;
        inWord = true;
      }
    }
    return count;
  }
}