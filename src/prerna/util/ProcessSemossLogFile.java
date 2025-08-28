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
package prerna.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ProcessSemossLogFile {

  public static void main(String[] args) throws IOException {
    String directory = "C:\\Users\\mahkhalil\\Downloads\\logs";
    File mainDir = new File(directory);

    ProcessSemossLogFile processor = new ProcessSemossLogFile();
    Map<String, List<String>> found = new TreeMap<>();
    processor.processDirectory(mainDir, "Running >>> InventoryImport", found);

    BufferedWriter writer =
        new BufferedWriter(new FileWriter("C:\\Users\\mahkhalil\\Downloads\\found.txt"));
    for (String filename : found.keySet()) {
      writer.write(filename);
      writer.write("\n");
      List<String> foundLines = found.get(filename);
      for (String line : foundLines) {
        writer.write(line);
        writer.write("\n");
      }
      writer.flush();
    }
    writer.close();
  }

  public void processDirectory(File directory, String textToFind, Map<String, List<String>> found)
      throws IOException {
    File[] files = directory.listFiles();
    for (File f : files) {
      if (f.isDirectory()) {
        processDirectory(f, textToFind, found);
      } else {
        processFile(f, textToFind, found);
      }
    }
  }

  private void processFile(File f, String textToFind, Map<String, List<String>> found)
      throws IOException {
    String filePath = f.getAbsolutePath();
    if (filePath.endsWith(".log")) {
      System.out.println("Process ::: " + filePath);
      readFiles(f, textToFind, found);
    } else {
      System.out.println("Ignore ::: " + filePath);
    }
  }

  private void readFiles(File f, String textToFind, Map<String, List<String>> found)
      throws IOException {
    List<String> foundValues = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(f))) {
      String line;
      while ((line = br.readLine()) != null) {
        // process the line
        if (line.contains(textToFind)) {
          System.out.println(line);
          foundValues.add(line);
        }
      }
    }
    found.put(f.getName(), foundValues);
  }
}
