package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDResources;
// added for images
import org.apache.pdfbox.pdmodel.graphics.PDXObject;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;
import org.apache.pdfbox.text.PDFTextStripper;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.Constants;

public class ImagePDFProcessor extends AbstractFileImageProcessor {

	private static final Logger classLogger = LogManager.getLogger(PDFProcessor.class);

	public ImagePDFProcessor(String filePath, VectorDatabaseCSVWriter writer) {
		super(filePath, writer);
	}

	@Override
	public void process() {
		try (PDDocument document = PDDocument.load(new File(this.filePath))) {
			PDFTextStripper stripper = new PDFTextStripper();
			String source = getSource(this.filePath);

			for (int pageIndex = 0; pageIndex < document.getNumberOfPages(); pageIndex++) {
				stripper.setStartPage(pageIndex + 1);
				stripper.setEndPage(pageIndex + 1);
				String text = stripper.getText(document);

				// Extract images
				PDPage page = document.getPage(pageIndex);
				List<String> imageIds = extractImages(page);
				classLogger.debug("Found {} images in {} on page {}", imageIds.size(), source, pageIndex);
				// Combine text and image placeholders
				String combinedContent = combineTextAndImages(text, imageIds);

				writer.writeRow(source, String.valueOf(pageIndex + 1), combinedContent);
			}
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	/**
	 * 
	 * @param page
	 * @return
	 * @throws IOException
	 */
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
						this.imageMap.put(imageId,  base64Image);
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

	/**
	 * 
	 * @param text
	 * @param imageIds
	 * @return
	 */
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

	/**
	 * 
	 * @param image
	 * @return
	 */
	private boolean isImageSizeAcceptable(PDImageXObject image) {
		return image.getWidth() >= MIN_IMAGE_WIDTH && image.getHeight() >= MIN_IMAGE_HEIGHT;
	}
	
}
