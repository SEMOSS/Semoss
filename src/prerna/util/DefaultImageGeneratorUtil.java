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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DefaultImageGeneratorUtil {

  private static final Logger classLogger = LogManager.getLogger(DefaultImageGeneratorUtil.class);

  /**
   * Picks a random image for an engine
   *
   * @param fileLocation
   * @return
   */
  public static File pickRandomImage(String fileLocation) {
    String baseDirectory =
        DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", "/");
    if (!baseDirectory.endsWith("/")) {
      baseDirectory = baseDirectory + "/";
    }
    String imageDir = baseDirectory + "images" + File.separator + "stock-engines";
    File f = new File(imageDir);
    String[] a = f.list();
    Random rand = new Random();

    int i = rand.nextInt(a.length);
    String newImage = a[i];

    File thisNewImage = new File(imageDir + File.separator + newImage);
    // make the file location directory if it doesn't already exist
    {
      File fileDir = new File(fileLocation).getParentFile();
      if (!fileDir.exists() || !fileDir.isDirectory()) {
        fileDir.mkdirs();
      }
    }
    Path p = thisNewImage.toPath();
    Path from = Paths.get(fileLocation);
    try {
      Files.copy(p, Files.newOutputStream(from));
    } catch (IOException e) {
      classLogger.error(Constants.STACKTRACE, e);
    }
    f = new File(fileLocation);
    return f;
  }
}
