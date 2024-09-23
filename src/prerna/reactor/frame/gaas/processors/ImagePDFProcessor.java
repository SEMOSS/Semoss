package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDResources;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.text.PDFTextStripper;


// added for images
import org.apache.pdfbox.pdmodel.graphics.PDXObject;
import org.apache.pdfbox.pdmodel.graphics.form.PDFormXObject;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.contentstream.operator.DrawObject;
import org.apache.pdfbox.text.TextPosition;
import java.util.List;
import java.util.ArrayList;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.Iterator;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.Constants;
import prerna.util.Utility;


public class ImagePDFProcessor {

    private static final Logger classLogger = LogManager.getLogger(PDFProcessor.class);

    private String filePath;
    private VectorDatabaseCSVWriter writer;
    private Map<String, String> imageMap;
    
    private static final int MIN_IMAGE_WIDTH = 300;
    private static final int MIN_IMAGE_HEIGHT = 300;

    public ImagePDFProcessor(String filePath, VectorDatabaseCSVWriter writer) {
        this.filePath = filePath;
        this.writer = writer;
        this.imageMap = new HashMap<>();
        
    }

    public void process() {
    	try (PDDocument document = PDDocument.load(new File(filePath))) {
    		PDFTextStripper stripper = new PDFTextStripper();
    		String source = getSource(filePath);
    		
    		for (int pageIndex = 0; pageIndex < document.getNumberOfPages(); pageIndex++) {
    			stripper.setStartPage(pageIndex + 1);
    			stripper.setEndPage(pageIndex + 1);
    			String text = stripper.getText(document);
    			
    			// Extract images
    			PDPage page = document.getPage(pageIndex);
    			List<String> imageIds = extractImages(page);
    			
    			// Combine text and image placeholders
    			String combinedContent = combineTextAndImages(text, imageIds);
    			
    			writer.writeRow(source, String.valueOf(pageIndex + 1), combinedContent, "");
    		}
    	} catch (IOException e) {
    		classLogger.error(Constants.STACKTRACE, e);
    	}
    }
    
    private List<String> extractImages(PDPage page) throws IOException {
    	List<String> imageIds = new ArrayList<>();
    	PDResources resources = page.getResources();
    	
    	for (COSName name : resources.getXObjectNames()) {
    		try {
	    		PDXObject xobject = resources.getXObject(name);
	    		if (xobject instanceof PDImageXObject) {
	    			PDImageXObject image = (PDImageXObject) xobject;
	    			if (isImageSizeAcceptable(image)) {
		    			String imageId = generateUniqueImageId();
		    			String base64Image = convertToBase64(image.getImage());
		    			imageMap.put(imageId,  base64Image);
		    			imageIds.add(imageId);
	    			}
	    		}
    		} catch (IOException e) {
    			classLogger.error("Error processing image: " + name, e);
    	
    		} catch (Exception e) {
    			classLogger.error("Unexpected error processing image: " + name, e);
    		}
    	}
    	return imageIds;
    }
    
    private String combineTextAndImages(String text, List<String> imageIds) {
    	StringBuilder combined = new StringBuilder();
    	String[] paragraphs = text.split("\n\n");
    	int imageIndex = 0;
    	
    	for (String paragraph : paragraphs) {
    		combined.append(paragraph).append("\n\n");
    		if (imageIndex < imageIds.size()) {
    			combined.append(imageIds.get(imageIndex)).append("\n\n");
    			imageIndex++;
    		}
    	}
    	while (imageIndex < imageIds.size()) {
    		combined.append(imageIds.get(imageIndex)).append("\n\n");
    		imageIndex++;
    	}
    	return combined.toString().trim();
    }
    
    
    private String generateUniqueImageId() {
        return "[[IMG:" + UUID.randomUUID().toString() + "]]";
    }
    
    private String convertToBase64(BufferedImage image) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(image, "png", baos);
        byte[] imageBytes = baos.toByteArray();
        return Base64.getEncoder().encodeToString(imageBytes);
    }
    
    private String getSource(String filePath) {
        String source = null;
        File file = new File(filePath);
        if (file.exists()) {
            source = file.getName();
        }
        return source;
    }
    
    private boolean isImageSizeAcceptable(PDImageXObject image) {
    	return image.getWidth() >= MIN_IMAGE_WIDTH && image.getHeight() >= MIN_IMAGE_HEIGHT;
    }
    
    public Map<String, String> getImageMap() {
        return imageMap;
    }


   
}



