package prerna.sablecc2.reactor.export.pdf;

import java.io.File;
import java.io.IOException;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.PDPageContentStream.AppendMode;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.interactive.annotation.PDAnnotationWidget;
import org.apache.pdfbox.pdmodel.interactive.form.PDAcroForm;
import org.apache.pdfbox.pdmodel.interactive.form.PDSignatureField;
/**
 * An example of creating an AcroForm and an empty signature field from scratch.
 * 
 * An actual signature can be added by clicking on it in Adobe Reader.
 * 
 */
public final class PDFUtility {

	/**
	 * Create PDDocument
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	public static PDDocument createDocument(String filePath) throws IOException {
		File file = new File(filePath);
		return PDDocument.load(file);
	}
	
	public static void addSignatureBlock(PDDocument document) throws IOException {
		// Get the last page index of document
    	int pageNum = document.getNumberOfPages();
    	PDPage lastPage = document.getPage(pageNum-1);

        // Add a new AcroForm and add that to the document
        PDAcroForm acroForm = new PDAcroForm(document);
        document.getDocumentCatalog().setAcroForm(acroForm);

        // Create empty signature field for each rectangle
    	PDSignatureField signatureField = new PDSignatureField(acroForm);
    	signatureField.setPartialName("Signature");
        PDAnnotationWidget widget = signatureField.getWidgets().get(0);
        PDRectangle rect = new PDRectangle(375, 20, 200, 40);
        widget.setRectangle(rect);
        widget.setPage(lastPage);
        widget.setPrinted(true);
        lastPage.getAnnotations().add(widget);
        acroForm.getFields().add(signatureField);
	}
	
	/**
	 * 
	 * @param document
	 * @throws IOException
	 */
	public static void addSignatureLabel(PDDocument document, String signatureLabel) throws IOException {
		// Get the last page index of document
		int pageNum = document.getNumberOfPages();
		PDPage lastPage = document.getPage(pageNum-1);
		
		PDPageContentStream contentStream = new PDPageContentStream(document, lastPage, AppendMode.APPEND, true);
		contentStream.beginText();
		contentStream.setFont(PDType1Font.TIMES_ROMAN, 12);
		contentStream.setLeading(16f);
		contentStream.newLineAtOffset(375, 65);
		contentStream.showText(signatureLabel);
		contentStream.endText();
		contentStream.close();
	}
	
	
	public static void createSignatureBlock(String filePath) throws IOException {
		PDDocument document = createDocument(filePath);
		try {
			addSignatureBlock(document);
            document.save(filePath);
		} finally {
			if(document != null) {
				document.close();
			}
		}
	}
	
	/**
	 * 
	 * @param filePath
	 * @throws IOException
	 */
	public static void createSignatureLabel(String filePath, String signatureLabel) throws IOException {
		PDDocument document = createDocument(filePath);
		try {
			addSignatureLabel(document, signatureLabel);
            document.save(filePath);
		} finally {
			if(document != null) {
				document.close();
			}
		}
	}
	
	/**
	 * 
	 * @param document
	 * @throws IOException
	 */
	public static void addPageNumbers(PDDocument document, int startingNumber, boolean ignoreFirstPage) throws IOException {
		int numPages = document.getNumberOfPages();
		for(int i = 0; i < numPages; i++) {
			if(i == 0 && ignoreFirstPage) {
				continue;
			}
			PDPage thisPage = document.getPage(i);
			
			PDPageContentStream contentStream = new PDPageContentStream(document, thisPage, AppendMode.APPEND, true);
			contentStream.beginText();
			contentStream.setFont(PDType1Font.TIMES_ROMAN, 8);
			contentStream.setLeading(16f);
			contentStream.newLineAtOffset(thisPage.getMediaBox().getWidth()/2, 10);
			contentStream.showText((startingNumber++) + "");
			contentStream.endText();
			contentStream.close();
		}
	}
}
