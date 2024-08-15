
package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.text.PDFTextStripper;

import com.mysql.cj.x.protobuf.MysqlxExpr.Operator;

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


import prerna.util.Constants;
import prerna.util.Utility;


public class ImagePDFProcessor {

    private static final Logger classLogger = LogManager.getLogger(PDFProcessor.class);

    private String filePath = null;
    private CSVWriter writer = null;
    private Map<String, String> imageMap;

    public ImagePDFProcessor(String filePath, CSVWriter writer) {
        this.filePath = filePath;
        this.writer = writer;
        this.imageMap = new HashMap<>();
        
    }

    public void process() {
        PDDocument pdDoc = null;
        try {
            File f = new File(this.filePath);
            String source = getSource(this.filePath);
            PDFImageTextStripper pdfStripper = new PDFImageTextStripper();
            pdDoc = PDDocument.load(f);
            int totalPages = pdDoc.getNumberOfPages();
            for (int pageIndex = 1; pageIndex <= totalPages; pageIndex++) {
                pdfStripper.setStartPage(pageIndex);
                pdfStripper.setEndPage(pageIndex);
                String parsedText = pdfStripper.getText(pdDoc);
                writer.writeRow(source, pageIndex + "", parsedText, "");
            }
        } catch (IOException e) {
            classLogger.error(Constants.STACKTRACE, e);
        } finally {
            if (pdDoc != null) {
                try {
                    pdDoc.close();
                } catch (IOException e) {
                    classLogger.error(Constants.STACKTRACE, e);
                }
            }
        }
    }
    public Map<String, String> getImageMap() {
        return imageMap;
    }

    private String getSource(String filePath) {
        String source = null;
        File file = new File(filePath);
        if (file.exists()) {
            source = file.getName();
        }
        return source;
    }

    private class PDFImageTextStripper extends PDFTextStripper {

        private List<ImagePlaceholder> imagePlaceholders = new ArrayList<>();
        private float currentY = -1;

        public PDFImageTextStripper() throws IOException {
            super();
            addOperator(new DrawObject());
        }
        
        @Override
        protected void writeString(String text, List<TextPosition> textPositions) throws IOException {
            for (TextPosition textPosition : textPositions) {
                insertImagePlaceholders(textPosition.getY());
                super.writeString(String.valueOf(textPosition.getUnicode()),  Collections.singletonList(textPosition));
            }
            currentY = textPositions.get(textPositions.size() - 1).getY();
        }
        
        protected void insertImagePlaceholders(float y) throws IOException {
            Iterator<ImagePlaceholder> iterator = imagePlaceholders.iterator();
            while (iterator.hasNext()) {
                ImagePlaceholder placeholder = iterator.next();
                if (placeholder.y <= y) {
                    super.writeString(placeholder.id, null);
                    iterator.remove();
                } else {
                    break;
                }
            }
        }

        @Override
        protected void processOperator(org.apache.pdfbox.contentstream.operator.Operator operator, List<COSBase> operands) throws IOException {
            String operation = operator.getName();
            if (operation.equals("Do")) {
                COSName objectName = (COSName) operands.get(0);
                PDXObject xobject = getResources().getXObject(objectName);
                if (xobject instanceof PDImageXObject) {
                    PDImageXObject imageXObject = (PDImageXObject) xobject;
                    String format = imageXObject.getSuffix();
                    if ("jpg".equalsIgnoreCase(format) || "jpeg".equalsIgnoreCase(format) || "png".equalsIgnoreCase(format)) {
                        String imageId = generateUniqueImageId();
                        String base64Image = extractImageData(xobject);
                        
                        imageMap.put(imageId,  base64Image);
                        imagePlaceholders.add(new ImagePlaceholder(imageId, currentY));
                    }

                }
            }
            super.processOperator(operator, operands);
        }
        
        @Override
        public String getText(PDDocument doc) throws IOException {
            imagePlaceholders.clear();
            currentY = -1;
            return super.getText(doc);
        }
        
        private String generateUniqueImageId() {
            return "[[IMG:" + UUID.randomUUID().toString() + "]]";
        }
        
        private String extractImageData(PDXObject xobject) throws IOException {
            if (xobject instanceof PDImageXObject) {
                PDImageXObject image = (PDImageXObject) xobject;
                BufferedImage bufferedImage = image.getImage();
                return convertToBase64(bufferedImage);
            } else if (xobject instanceof PDFormXObject) {
                // For form XObjects, we'll store metadata
                PDFormXObject form =  (PDFormXObject) xobject;
                String metadata = String.format("Form XObject: Width=%.2f, Height=%.2f",
                        form.getBBox().getWidth(), form.getBBox().getHeight());
                return Base64.getEncoder().encodeToString(metadata.getBytes());
        }
            return "";
    }
        private String convertToBase64(BufferedImage image) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(image, "png", baos);
            byte[] imageBytes = baos.toByteArray();
            return Base64.getEncoder().encodeToString(imageBytes);
        }
        
        private class ImagePlaceholder {
            String id;
            float y;
            
            ImagePlaceholder(String id, float y) {
                this.id = id;
                this.y = y;
            }
        }
}
}


