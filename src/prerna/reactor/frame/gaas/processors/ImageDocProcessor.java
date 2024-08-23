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
import org.apache.poi.xwpf.usermodel.XWPFTableCell;
import org.apache.poi.xwpf.usermodel.ICell;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.Constants;

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
			
			for (XWPFRun run : paragraph.getRuns()) {
				paragraphText.append(run.getText(0));
				
				if (embedImages) {
					List<XWPFPicture> pictures = run.getEmbeddedPictures();
					for (XWPFPicture picture : pictures) {
						if (isImageSizeAcceptable(picture)) {
							String imageId = processImage(picture);
							paragraphText.append(" ").append(imageId).append(" ");
						}
					}
				}
			}
			
			String text = paragraphText.toString().trim();
			if (!text.isEmpty()) {
				writer.writeRow(source, count + "", text, pageNo + "");
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
			BufferedImage image = ImageIO.read(new ByteArrayInputStream(picture.getPictureData().getData()));
			return image.getWidth() >= MIN_IMAGE_WIDTH && image.getHeight() >= MIN_IMAGE_HEIGHT;
		} catch (IOException e) {
			classLogger.error("Error reading image dimensions", e);
			return false;
		}
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
