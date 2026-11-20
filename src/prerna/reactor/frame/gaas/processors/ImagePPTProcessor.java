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

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFShape;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFTextShape;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;

public class ImagePPTProcessor extends AbstractFileImageProcessor {

	private static final Logger classLogger = LogManager.getLogger(PPTProcessor.class);

	public ImagePPTProcessor(String filePath, VectorDatabaseCSVWriter writer) {
		super(filePath, writer);
	}

	@Override
	public void process() {
		try (FileInputStream is = new FileInputStream(this.filePath); XMLSlideShow ppt = new XMLSlideShow(is);) {
			processSlides(ppt);
		} catch (IOException e) {
			classLogger.error("Failed to process PPT file into images: {}", e.getMessage(), e);
			return;
		}
	}

	private void processSlides(XMLSlideShow ppt) {
		String source = getSource(this.filePath);
		Dimension pgsize = ppt.getPageSize();
		int slideCount = 1;

		for (XSLFSlide slide : ppt.getSlides()) {
			BufferedImage img = new BufferedImage(pgsize.width, pgsize.height, BufferedImage.TYPE_INT_RGB);
			Graphics2D graphics = img.createGraphics();

			try {
				renderSlide(slide, graphics, pgsize);
			} catch (NoClassDefFoundError | ClassNotFoundException e) {
				renderFallbackSlide(slide, graphics, pgsize);
				classLogger.warn("Used fallback rendering for slide " + slideCount + " due to " + e.getMessage());
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
		graphics.drawString("Slide content may be incomplete due to rendering limitations", 50, pgsize.height - 50);
	}

}