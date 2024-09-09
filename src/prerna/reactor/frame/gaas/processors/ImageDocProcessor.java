package prerna.reactor.frame.gaas.processors;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;

import javax.imageio.ImageIO;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFPictureData;
import org.apache.poi.xwpf.usermodel.XWPFPicture;
import org.apache.poi.xwpf.usermodel.XWPFRun;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableRow;
import org.apache.tomcat.util.http.fileupload.ByteArrayOutputStream;
import org.apache.poi.xwpf.usermodel.XWPFTableCell;
import org.apache.poi.xwpf.usermodel.ICell;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.Constants;

import org.apache.commons.imaging.ImageFormats;
import org.apache.commons.imaging.Imaging;

public class ImageDocProcessor {

	private static final Logger classLogger = LogManager.getLogger(ImageDocProcessor.class);
	private String filePath;
	private VectorDatabaseCSVWriter writer;
	private boolean embedImages;
	private Map<String, String> imageMap;
	
	// Define the min image dimensions
	private static final int MIN_IMAGE_WIDTH = 300;
	private static final int MIN_IMAGE_HEIGHT  = 300;
	
	public ImageDocProcessor(String filePath, VectorDatabaseCSVWriter writer, boolean embedImages) {
		this.filePath = filePath;
		this.writer = writer;
		this.embedImages = embedImages;
		this.imageMap = new HashMap<>();
	}
	
	public void process() {
		FileInputStream is = null;
		XWPFDocument document = null;
		
		try {
			is = new FileInputStream(filePath);
			document = new XWPFDocument(is);
			
			processParagraphs(document);
			processTables(document);
		
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (document != null) {
				try {
					document.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	private void processParagraphs(XWPFDocument document) {
		int count = 1;
		int pageNo = 1;
		String source = getSource(filePath);
		
		for (XWPFParagraph paragraph : document.getParagraphs()) {
			StringBuilder paragraphText = new StringBuilder();
			
			// check if paragraph or its runs are null
			if (paragraph == null || paragraph.getRuns() == null) {
				continue; // Skip this paragraph as well
			}
			
			for (XWPFRun run : paragraph.getRuns()) {
				paragraphText.append(run.getText(0));
				
				if (embedImages) {
					// Safely handle embedded images
					List<XWPFPicture> pictures = run.getEmbeddedPictures();
					if (pictures != null) {
						for (XWPFPicture picture : pictures) {
							if (picture != null && isImageSizeAcceptable(picture)) {
								String imageId = processImage(picture);
								if (imageId != null) {
									paragraphText.append(" ").append(imageId).append(" ");
								}
							}
						}
					}
				}
			}
			
			String text = paragraphText.toString().trim();
			if (!text.isEmpty()) {
				writer.writeRow(source, String.valueOf(count), text, String.valueOf(pageNo));
			}
			
			if (paragraph.isPageBreak()) {
				pageNo++;
			}
			count++;
		}
	}
	
	private void processTables(XWPFDocument document) {
		int count = 1;
		int pageNo = 1;
		String source = getSource(filePath);
		
		String[] headers = null;
		String[] values = null;
		boolean headerProcessed = false;
		
		for (XWPFTable table : document.getTables()) {
			List<XWPFTableRow> rows = table.getRows();
			for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
				XWPFTableRow row = table.getRow(rowIndex);
				List<ICell> cells = row.getTableICells();
				String[] processor = new String[cells.size()];
				for (int cellIndex = 0; cellIndex < cells.size(); cellIndex++) {
					ICell thisCell = cells.get(cellIndex);
					if (thisCell instanceof XWPFTableCell) {
						processor[cellIndex] = processTableCell((XWPFTableCell) thisCell);
					}
				}
				if (!headerProcessed) {
					headers = processor;
					headerProcessed = true;
				} else {
					values = processor;
					StringBuilder rowOut = getRow(headers, values);
					writer.writeRow(source, count + "", rowOut + "", pageNo + "'");
				}
			}
			headerProcessed = false;
			count++;
		}
	}
	
	private String processTableCell(XWPFTableCell cell) {
		StringBuilder cellContent = new StringBuilder(cell.getText());
		if (embedImages) {
			for (XWPFParagraph paragraph : cell.getParagraphs()) {
				for (XWPFRun run : paragraph.getRuns()) {
					List<XWPFPicture> pictures = run.getEmbeddedPictures();
					for (XWPFPicture picture : pictures) {
						if (isImageSizeAcceptable(picture)) {
							String imageId = processImage(picture);
							cellContent.append(" ").append(imageId).append(" ");
						}
					}
				}
			}
		}
		return cellContent.toString();
	}
	
	private StringBuilder getRow(String[] headers, String[] values) {
		StringBuilder builder = new StringBuilder();
		for (int valIndex = 0; valIndex < values.length; valIndex++) {
			String header = null;
			if (valIndex < headers.length) {
				header = headers[valIndex];
			} else {
				header = headers[headers.length - 1];
			}
			builder.append(header).append("=").append(values[valIndex]).append(" ");
		}
		return builder;
	}
	
	private boolean isImageSizeAcceptable(XWPFPicture picture) {
		try {
			byte[] imageData = picture.getPictureData().getData();
			
			// Check if image data is null or empty
			if (imageData == null || imageData.length == 0) {
				classLogger.error("Image data is null or empty.");
				return false;
			}
			
			String format = picture.getPictureData().suggestFileExtension();
			
			BufferedImage image;
			if (isSupportedFormat(format)) {
				image = ImageIO.read(new ByteArrayInputStream(imageData));
			} else {
				// If not supported, attempt to convert to PNG
				image = convertToSupportedFormat(imageData, format);
			}
			
			if (image == null) {
				classLogger.error("Failed to convert or read the image. Unsupported format or corrupted data.");
				return false;
			}
			
			// Validate image size
			return image.getWidth() >= MIN_IMAGE_WIDTH && image.getHeight() >= MIN_IMAGE_HEIGHT;
			
		} catch (IOException e) {
			classLogger.error("Error reading or converting image dimensions", e);
			return false;
			
		} catch (Exception e) {
			classLogger.error("Unexpected error while processing image", e);
			return false;
		}
	}
	
	// Helper method to check if the image format is supported by ImageIO
	private boolean isSupportedFormat(String format) {
		String[] supportedFormats = ImageIO.getReaderFormatNames();
		for (String supported : supportedFormats) {
			if (supported.equalsIgnoreCase(format)) {
				return true;
			}
		}
		return false;
	}
	
	// Method to convert unsupported image formats to a supported one
	private BufferedImage convertToSupportedFormat(byte[] imageData, String format) {
		try {
			// Convert the image data to a BufferedImage using Apache Commons Imaging
			BufferedImage image = Imaging.getBufferedImage(imageData);
			if (image != null) {
				// Reencode the image as PNG
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				Imaging.writeImage(image,  baos, ImageFormats.PNG, null);
				return ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
			}
		} catch (Exception e) {
			classLogger.error("Error converting image from format: " + format, e);
		}
		return null;
	}
	
	private String processImage(XWPFPicture picture) {
		String imageId = generateUniqueImageId();
		String base64Image = Base64.getEncoder().encodeToString(picture.getPictureData().getData());
		imageMap.put(imageId, base64Image);
		return imageId;
	}
	
    private String generateUniqueImageId() {
        return "[[IMG:" + UUID.randomUUID().toString() + "]]";
    }
    
    private String getSource(String filePath) {
        String source = null;
        File file = new File(filePath);
        if (file.exists()) {
            source = file.getName();
        }
        return source;
    }
    
    public Map<String, String> getImageMap() {
    	return imageMap;
    }
	
	
}
