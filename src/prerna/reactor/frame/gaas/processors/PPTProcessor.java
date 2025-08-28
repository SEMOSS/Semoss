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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.hslf.usermodel.HSLFNotes;
import org.apache.poi.hslf.usermodel.HSLFShape;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;
import org.apache.poi.hslf.usermodel.HSLFTextShape;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFNotes;
import org.apache.poi.xslf.usermodel.XSLFShape;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFTextParagraph;
import org.apache.poi.xslf.usermodel.XSLFTextShape;
import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.Constants;

public class PPTProcessor extends AbstractFileProcessor {

  private static final Logger classLogger = LogManager.getLogger(PPTProcessor.class);

  /**
   * @param filePath
   * @param writer
   */
  public PPTProcessor(String filePath, VectorDatabaseCSVWriter writer) {
    super(filePath, writer);
  }

  @Override
  public void process() throws IOException {
    FileInputStream is = null;
    Object ppt = null; // To hold either HSLFSlideShow or XMLSlideShow
    try {
      String fileType = FilenameUtils.getExtension(this.filePath);

      is = new FileInputStream(this.filePath);
      if (fileType.equalsIgnoreCase("ppt")) {
        ppt = new HSLFSlideShow(is);
        processSlides((HSLFSlideShow) ppt);
      } else {
        ppt = new XMLSlideShow(is);
        processSlides((XMLSlideShow) ppt);
      }
    } catch (IOException e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw e;
    } finally {
      closeDocument(ppt);
      closeInputStream(is);
    }
  }

  private void closeDocument(Object ppt) {
    if (ppt != null) {
      try {
        if (ppt instanceof HSLFSlideShow) {
          ((HSLFSlideShow) ppt).close();
        } else if (ppt instanceof XMLSlideShow) {
          ((XMLSlideShow) ppt).close();
        }
      } catch (IOException e) {
        classLogger.error("Error closing PowerPoint document", e);
      }
    }
  }

  private void closeInputStream(FileInputStream is) {
    if (is != null) {
      try {
        is.close();
      } catch (IOException e) {
        classLogger.error("Error closing FileInputStream", e);
      }
    }
  }

  /**
   * @param ppt
   */
  private void processSlides(XMLSlideShow ppt) {
    //      CoreProperties props = ppt.getProperties().getCoreProperties();
    //      String title = props.getTitle();
    String source = getSource(this.filePath);
    int count = 1;
    for (XSLFSlide slide : ppt.getSlides()) {
      StringBuilder slideText = new StringBuilder();

      List<XSLFShape> shapes = slide.getShapes();
      for (XSLFShape shape : shapes) {
        if (shape instanceof XSLFTextShape) {
          XSLFTextShape textShape = (XSLFTextShape) shape;
          String text = textShape.getText();
          slideText.append(text);
          // System.out.println("Text: " + text);
        }
      }

      // get the notes
      XSLFNotes mynotes = slide.getNotes();
      if (mynotes != null) {
        for (XSLFShape shape : mynotes) {
          if (shape instanceof XSLFTextShape) {
            XSLFTextShape txShape = (XSLFTextShape) shape;
            for (XSLFTextParagraph xslfParagraph : txShape.getTextParagraphs()) {
              String text = xslfParagraph.getText();
              slideText.append(text);
            }
          }
        }
      }
      this.writer.writeRow(source, count + "", slideText.toString());
      count++;
    }
  }

  private void processSlides(HSLFSlideShow ppt) {
    String source = getSource(this.filePath);
    int count = 1;

    for (HSLFSlide slide : ppt.getSlides()) {
      StringBuilder slideText = new StringBuilder();
      List<HSLFShape> shapes = slide.getShapes();

      for (HSLFShape shape : shapes) {
        if (shape instanceof HSLFTextShape) {
          HSLFTextShape textShape = (HSLFTextShape) shape;
          // Get the complete text from the text shape
          slideText.append(textShape.getText()); // This retrieves all text in the shape
          slideText.append("\n"); //  Add a newline for better formatting
        }
      }

      HSLFNotes notes = slide.getNotes();
      if (notes != null) {
        for (HSLFShape shape : notes.getShapes()) {
          if (shape instanceof HSLFTextShape) {
            HSLFTextShape textShape = (HSLFTextShape) shape;
            // Get the complete text from the notes text shape
            slideText.append(textShape.getText()); // This retrieves all text in the shape
            slideText.append("\n"); // Optional: Add a newline for better formatting
          }
        }
      }

      this.writer.writeRow(source, count + "", slideText.toString());
      count++;
    }
  }
}
