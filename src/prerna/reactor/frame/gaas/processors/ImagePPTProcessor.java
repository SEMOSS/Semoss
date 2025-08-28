/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.reactor.frame.gaas.processors;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFShape;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFTextShape;
import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.Constants;

public class ImagePPTProcessor extends AbstractFileImageProcessor {

  private static final Logger classLogger = LogManager.getLogger(PPTProcessor.class);

  public ImagePPTProcessor(String filePath, VectorDatabaseCSVWriter writer) {
    super(filePath, writer);
  }

  public void process() {
    FileInputStream is = null;
    XMLSlideShow ppt = null;
    try {
      try {
        is = new FileInputStream(this.filePath);
      } catch (FileNotFoundException e) {
        classLogger.error(Constants.STACKTRACE, e);
        return;
      }
      try {
        ppt = new XMLSlideShow(is);
      } catch (IOException e) {
        classLogger.error(Constants.STACKTRACE, e);
        return;
      }
      processSlides(ppt);
    } finally {
      if (ppt != null) {
        try {
          is.close();
        } catch (IOException e) {
          classLogger.error(Constants.STACKTRACE, e);
        }
      }
    }
  }

  private void processSlides(XMLSlideShow ppt) {
    String source = getSource(this.filePath);
    Dimension pgsize = ppt.getPageSize();
    int slideCount = 1;

    for (XSLFSlide slide : ppt.getSlides()) {
      BufferedImage img =
          new BufferedImage(pgsize.width, pgsize.height, BufferedImage.TYPE_INT_RGB);
      Graphics2D graphics = img.createGraphics();

      try {
        renderSlide(slide, graphics, pgsize);
      } catch (NoClassDefFoundError | ClassNotFoundException e) {
        renderFallbackSlide(slide, graphics, pgsize);
        classLogger.warn(
            "Used fallback rendering for slide " + slideCount + " due to " + e.getMessage());
      }

      // Convert the image to base64
      String imageId = generateUniqueImageId();
      String base64Image = convertToBase64(img);
      imageMap.put(imageId, base64Image);

      // Write to CSV
      this.writer.writeRow(source, String.valueOf(slideCount), imageId);

      slideCount++;
      graphics.dispose();
    }
  }

  private void renderSlide(XSLFSlide slide, Graphics2D graphics, Dimension pgsize)
      throws NoClassDefFoundError, ClassNotFoundException {
    // Clear the drawing area
    graphics.setPaint(slide.getBackground().getFillColor());
    graphics.fill(new java.awt.Rectangle(0, 0, pgsize.width, pgsize.height));

    slide.draw(graphics);
  }

  private void renderFallbackSlide(XSLFSlide slide, Graphics2D graphics, Dimension pgsize) {
    // Clear the drawing area
    graphics.setPaint(slide.getBackground().getFillColor());
    graphics.fill(new java.awt.Rectangle(0, 0, pgsize.width, pgsize.height));

    // Draw a basic representation of shapes and text
    graphics.setPaint(Color.BLACK);
    int yOffset = 50;
    for (XSLFShape shape : slide.getShapes()) {
      if (shape instanceof XSLFTextShape) {
        XSLFTextShape textShape = (XSLFTextShape) shape;
        graphics.drawString(textShape.getText(), 50, yOffset);
        yOffset += 30;
      } else {
        graphics.drawRect(50, yOffset, 100, 50);
        yOffset += 70;
      }
    }
    graphics.drawString(
        "Slide content may be incomplete due to rendering limitations", 50, pgsize.height - 50);
  }
}
