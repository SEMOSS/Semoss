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
package prerna.reactor.frame.gaas.processors;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.imageio.ImageIO;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.Constants;

public abstract class AbstractFileImageProcessor extends AbstractFileProcessor
    implements IFileImageProcessor {

  private static final Logger classLogger = LogManager.getLogger(AbstractFileImageProcessor.class);

  // Define the min image dimensions
  protected static final int MIN_IMAGE_WIDTH = 300;
  protected static final int MIN_IMAGE_HEIGHT = 300;

  protected Map<String, String> imageMap;

  public AbstractFileImageProcessor(String filePath, VectorDatabaseCSVWriter writer) {
    super(filePath, writer);
    this.imageMap = new HashMap<>();
  }

  @Override
  public Map<String, String> getImageMap() {
    return imageMap;
  }

  /**
   * @return
   */
  protected String generateUniqueImageId() {
    return "[[IMG:" + UUID.randomUUID().toString() + "]]";
  }

  /**
   * @param image
   * @return
   */
  protected String convertToBase64(BufferedImage image) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ImageIO.write(image, "png", baos);
      byte[] imageBytes = baos.toByteArray();
      return Base64.getEncoder().encodeToString(imageBytes);
    } catch (IOException e) {
      classLogger.error("Error converting image to Base64", e);
      return "";
    }
  }

  /**
   * @param file
   * @param writer
   * @return
   */
  public static IFileImageProcessor getFileProcessor(File file, VectorDatabaseCSVWriter writer) {
    // pick up the files and convert them to CSV
    classLogger.info("Processing file : " + file.getName());

    // process this file
    String filetype = FilenameUtils.getExtension(file.getAbsolutePath());
    String mimeType = null;

    // using tika for mime type check since it is more consistent across env + rhel
    // OS and macOS
    TikaConfig config = TikaConfig.getDefaultConfig();
    Detector detector = config.getDetector();
    Metadata metadata = new Metadata();
    metadata.add(TikaCoreProperties.RESOURCE_NAME_KEY, file.getName());
    try (TikaInputStream stream = TikaInputStream.get(new FileInputStream(file))) {
      mimeType = detector.detect(stream, metadata).toString();
    } catch (IOException e) {
      classLogger.error(Constants.ERROR_MESSAGE, e);
    }

    if (mimeType == null) {
      throw new NullPointerException("Unable to determine the mimType for file " + file.getName());
    }

    IFileImageProcessor processor = null;

    classLogger.info("Processing file : " + file.getName() + " mime type: " + mimeType);
    if (mimeType.equalsIgnoreCase(
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        || ((mimeType.equalsIgnoreCase("application/x-tika-ooxml")
                || mimeType.equalsIgnoreCase("application/msword")
                || mimeType.equalsIgnoreCase("application/x-tika-msoffice"))
            && (filetype.equals("doc") || filetype.equals("docx")))) {
      // document
      processor = new ImageDocProcessor(file.getAbsolutePath(), writer);
    } else if (mimeType.equalsIgnoreCase(
            "application/vnd.openxmlformats-officedocument.presentationml.presentation")
        || ((mimeType.equalsIgnoreCase("application/x-tika-ooxml")
                || (mimeType.equalsIgnoreCase("application/vnd.ms-powerpoint")))
            && (filetype.equals("ppt") || filetype.equals("pptx")))) {
      // powerpoint
      processor = new ImagePPTProcessor(file.getAbsolutePath(), writer);
    } else if (mimeType.equalsIgnoreCase("application/pdf")) {
      processor = new ImagePDFProcessor(file.getAbsolutePath(), writer);
    } else {
      classLogger.warn("No support exists for parsing mime-type = " + mimeType);
      classLogger.warn("No support exists for parsing mime-type = " + mimeType);
      classLogger.warn("No support exists for parsing mime-type = " + mimeType);
      classLogger.warn("No support exists for parsing mime-type = " + mimeType);
      classLogger.warn("No support exists for parsing mime-type = " + mimeType);
      classLogger.warn("No support exists for parsing mime-type = " + mimeType);
      classLogger.warn("No support exists for parsing mime-type = " + mimeType);
    }

    return processor;
  }
}
