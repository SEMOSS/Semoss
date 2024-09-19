package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.awt.Dimension;
import java.awt.Graphics2D;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import javax.imageio.ImageIO;
import java.util.Base64;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.xslf.usermodel.*;
import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.Constants;

public class ImagePPTProcessor {

	private static final Logger classLogger = LogManager.getLogger(PPTProcessor.class);
	private String filePath = null;
	private VectorDatabaseCSVWriter writer = null;
	private Map<String, String> imageMap;
	
	public ImagePPTProcessor(String filePath, VectorDatabaseCSVWriter writer, boolean embedImages) {
		this.filePath = filePath;
		this.writer = writer;
		this.imageMap = new HashMap<>();
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
			this.writer.writeRow(source,  String.valueOf(slideCount), imageId, "");
			
			slideCount++;
			graphics.dispose();
		}
	}
	
	private void renderSlide(XSLFSlide slide, Graphics2D graphics, Dimension pgsize) throws NoClassDefFoundError, ClassNotFoundException {
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
	
	private String getSource(String filePath) {
		String source = null;
		File file = new File(filePath);
		if (file.exists()) {
			source = file.getName();
		}
		return source;
	}
	
    private String generateUniqueImageId() {
        return "[[IMG:" + UUID.randomUUID().toString() + "]]";
    }
    
    private String convertToBase64(BufferedImage image) {
    	try {
    		ByteArrayOutputStream baos = new ByteArrayOutputStream();
    		ImageIO.write(image,  "png", baos);
    		byte[] imageBytes = baos.toByteArray();
    		return Base64.getEncoder().encodeToString(imageBytes);
    	} catch (IOException e) {
    		classLogger.error("Error converting image to Base64", e);
    		return "";
    	}
    }
    
    public Map<String, String> getImageMap() {
    	return imageMap;
    }
	
}