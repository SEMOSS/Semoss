package prerna.reactor.export.pdf;

import java.awt.Color;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.multipdf.PDFMergerUtility;
import org.apache.pdfbox.multipdf.Splitter;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentNameDictionary;
import org.apache.pdfbox.pdmodel.PDEmbeddedFilesNameTreeNode;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.PDPageContentStream.AppendMode;
import org.apache.pdfbox.pdmodel.PDResources;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.common.filespecification.PDComplexFileSpecification;
import org.apache.pdfbox.pdmodel.common.filespecification.PDEmbeddedFile;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.interactive.annotation.PDAnnotationWidget;
import org.apache.pdfbox.pdmodel.interactive.annotation.PDAppearanceDictionary;
import org.apache.pdfbox.pdmodel.interactive.annotation.PDAppearanceEntry;
import org.apache.pdfbox.pdmodel.interactive.annotation.PDAppearanceStream;
import org.apache.pdfbox.pdmodel.interactive.form.PDAcroForm;
import org.apache.pdfbox.pdmodel.interactive.form.PDCheckBox;
import org.apache.pdfbox.pdmodel.interactive.form.PDSignatureField;
import org.apache.pdfbox.pdmodel.interactive.form.PDTextField;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.TextPosition;
import org.jsoup.nodes.Attributes;

import prerna.reactor.export.mustache.MustacheUtility;
import prerna.reactor.export.pdf.PDFUtility.FormObject;
import prerna.reactor.export.pdf.PDFUtility.RectanglePage;
import prerna.reactor.export.pdf.PDFUtility.pageLocation;
import prerna.sablecc2.om.ReactorKeysEnum;



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
		
	public static void addSignatureBlock(PDDocument document, FormObject rectPage) throws IOException {
		PDAcroForm acroForm = document.getDocumentCatalog().getAcroForm();
        PDRectangle rect = rectPage.rect;

		if (acroForm == null) {
	        // Add a new AcroForm and add that to the document
			acroForm = new PDAcroForm(document);
		}
        document.getDocumentCatalog().setAcroForm(acroForm);
        // Create empty signature field for each rectangle
    	PDSignatureField signatureField = new PDSignatureField(acroForm);
    	// Warning - partial name must be unique to each signature field
    	//signatureField.setPartialName("Signature");
        PDAnnotationWidget widget = signatureField.getWidgets().get(0);       
		// Get the sign page index of document
        int page = rectPage.page;
        PDPage signPage = document.getPage(page-1);
        widget.setRectangle(rect);
        widget.setPage(signPage);
        widget.setPrinted(true);
        signPage.getAnnotations().add(widget);
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
	 * Identify location of specific string in a document on a specific page
	 */
	static List<TextPositionSequence> findSubwords(PDDocument document, int page, List<String> searchTerms) throws IOException
	{
	    final List<TextPositionSequence> hits = new ArrayList<TextPositionSequence>();
	    PDFTextStripper stripper = new PDFTextStripper()
	    {
	        @Override
	        protected void writeString(String text, List<TextPosition> textPositions) throws IOException
	        {
	            TextPositionSequence word = new TextPositionSequence(textPositions);
	            String string = word.toString();

	            int fromIndex = 0;
	            int index;
	            
	            for ( String searchTerm : searchTerms ) {
		            while ((index = string.indexOf(searchTerm, fromIndex)) > -1)
		            {
		                hits.add(word.subSequence(index, index + searchTerm.length()));
		                fromIndex = index + 1;
		            }
	            }
	            
	            super.writeString(text, textPositions);
	        }
	    };
	    
	    stripper.setSortByPosition(true);
	    stripper.setStartPage(page);
	    stripper.setEndPage(page);
	    stripper.getText(document);
	    return hits;
	}
	
	/**
	 * 
	 * Object class returned by findWord
	 */
	public static class RectanglePage {
		public PDRectangle rect;
		public final int page;
		public String keyword;
		
		
		public RectanglePage(PDRectangle rect, int page, String keyword) {
			this.rect = rect;
			this.page = page;
			this.keyword = keyword;
		}
		
		public void setRectangle(PDRectangle newRect) {
			this.rect = newRect;
		}
		
		public PDRectangle getRectangle() {
			return this.rect;
		}
		
		public void setKeyword(String newKeyword) {
			this.keyword = newKeyword;
		}
		
		public String getKeyword() {
			return this.keyword;
		}
		
	}
	
	public static class FormObject {
		public PDRectangle rect;
		public final int page;
		public String keyword;
		public String formType;
		
		public FormObject(PDRectangle rect, int page, String keyword) {
			this.rect = rect;
			this.page = page;
			this.keyword = keyword;
		}		
		
		public FormObject(PDRectangle rect, int page, String keyword, String formType) {
			this.rect = rect;
			this.page = page;
			this.keyword = keyword;
			this.formType = formType;
		}
		
		public void setRectangle(PDRectangle newRect) {
			this.rect = newRect;
		}
		
		public PDRectangle getRectangle() {
			return this.rect;
		}
		
		public void setKeyword(String newKeyword) {
			this.keyword = newKeyword;
		}
		
		public String getKeyword() {
			return this.keyword;
		}
		
	}
	
	public static class pageLocation {
		public float xCoord;
		public float yCoord;
		public final int page;
		public String keyword;
		
		public pageLocation(float xCoord, float yCoord, int page, String keyword) {
			this.xCoord = xCoord;
			this.yCoord = yCoord;
			this.page = page;
			this.keyword = keyword;
		}
		
		public void setKeyword(String newKeyword) {
			this.keyword = newKeyword;
		}
		
		public String getKeyword() {
			return this.keyword;
		}
		
	}
	
	/**
	 * 
	 * Function returns Rectangle and Page to use for placing signature field within PDF
	 * 
	 */
	/**
	public static ArrayList<RectanglePage> findWordLocation(PDDocument document, List<String> searchTerms) throws IOException
	{	
		float x = 0;
		float y = 0;
		int pageFound = 0;
		
		ArrayList<RectanglePage> ret = new ArrayList();
	    //System.out.printf("* Looking for '%s'\n", searchTerm);
	    
		for (int page = 1; page <= document.getNumberOfPages(); page++) {
	        List<TextPositionSequence> hits = findSubwords(document, page, searchTerms);
	        for (TextPositionSequence hit : hits) {
		        x = hit.getX();
		        y = hit.textPositions.get(0).getPageHeight() - hit.getY();
		        pageFound = page;
		        PDRectangle rect = new PDRectangle(x, y - 42, 180, 40);
		        RectanglePage addToList = new RectanglePage(rect, pageFound, hit.toString());		        
		        ret.add(addToList);	        
	        }
	    }

	    return ret;
	}
	*/
	
	/**
	 * 
	 * @param document
	 * @param searchTerms
	 * @return
	 * @throws IOException
	 * 
	 * Returns corresponding page location list object for search term list object
	 * 
	 */
	public static ArrayList<pageLocation> findWordLocation(PDDocument document, List<String> searchTerms) throws IOException
	{	
		float x = 0;
		float y = 0;
		int pageFound = 0;
		
		ArrayList<pageLocation> ret = new ArrayList();
	    //System.out.printf("* Looking for '%s'\n", searchTerm);
	    
		for (int page = 1; page <= document.getNumberOfPages(); page++) {
	        List<TextPositionSequence> hits = findSubwords(document, page, searchTerms);
	        for (TextPositionSequence hit : hits) {
		        x = hit.getX();
		        y = hit.textPositions.get(0).getPageHeight() - hit.getY();
		        pageFound = page;
		        pageLocation addToList = new pageLocation(x, y, pageFound, hit.toString());		        
		        ret.add(addToList);	        
	        }
	    }

	    return ret;
	}
	
	
	/**
	 * 
	 * Helper class
	 */
	public static class TextPositionSequence implements CharSequence
	{
	    public TextPositionSequence(List<TextPosition> textPositions)
	    {
	        this(textPositions, 0, textPositions.size());
	    }

	    public TextPositionSequence(List<TextPosition> textPositions, int start, int end)
	    {
	        this.textPositions = textPositions;
	        this.start = start;
	        this.end = end;
	    }

	    @Override
	    public int length()
	    {
	        return end - start;
	    }

	    @Override
	    public char charAt(int index)
	    {
	        TextPosition textPosition = textPositionAt(index);
	        String text = textPosition.getUnicode();
	        return text.charAt(0);
	    }

	    @Override
	    public TextPositionSequence subSequence(int start, int end)
	    {
	        return new TextPositionSequence(textPositions, this.start + start, this.start + end);
	    }

	    @Override
	    public String toString()
	    {
	        StringBuilder builder = new StringBuilder(length());
	        for (int i = 0; i < length(); i++)
	        {
	            builder.append(charAt(i));
	        }
	        return builder.toString();
	    }

	    public TextPosition textPositionAt(int index)
	    {
	        return textPositions.get(start + index);
	    }

	    public float getX()
	    {
	        return textPositions.get(start).getXDirAdj();
	    }

	    public float getY()
	    {
	        return textPositions.get(start).getYDirAdj();
	    }

	    public float getWidth()
	    {
	        if (end == start)
	            return 0;
	        TextPosition first = textPositions.get(start);
	        TextPosition last = textPositions.get(end - 1);
	        return last.getWidthDirAdj() + last.getXDirAdj() - first.getXDirAdj();
	    }

	    final List<TextPosition> textPositions;
	    final int start, end;
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
	
	public static String parseStyleAttr(String style, String styleAttr) {
		styleAttr = styleAttr + ":";
		styleAttr = styleAttr.toLowerCase();
		style = style.toLowerCase();
		String attr = "";

		if ( style.lastIndexOf(styleAttr) >= 0 ) {
			attr = style.substring(style.lastIndexOf(styleAttr) + styleAttr.length()).trim();
			attr = attr.substring(0, attr.indexOf(";"));
			attr = attr.replace("px", "");
			attr = attr.replace("em", "");
		}
		
		return attr;
	}
	
	/**
	 * Converts to PDF pts (pts = px * inch / px * pt / inch)
	 * 
	 * @param px
	 * @return
	 */
	public static float convertToPts(float px) {
		return 72 * px / 96;
	}
	
	/**
	 * Sets the form object location based on page location and element attributes
	 * 
	 * @param elementAttributes
	 * @param pageLocationList
	 * @return
	 * @throws IOException
	 */
	public static ArrayList<FormObject> setFormObjectLocation(List<Attributes> elementAttributes, List<pageLocation> pageLocationList) throws IOException {
		String heightS;
		String widthS;
		String keyword;
		String formType;
		String xShiftS;
		String yShiftS;
		float heightSize;
		float widthSize;
		float xShift;
		float yShift;
		int index = 0;
		float LLX;
		float LLY;
		int pageNum;
		ArrayList<FormObject> formObjectList = new ArrayList<FormObject>();
		
		
		// we are going through this list more than we have page location objects
		for (Attributes elementAttrs : elementAttributes) {
			
			formType = (String) elementAttrs.get("smss-type");
			if (formType.equals("")) {			
				formType = "textbox";
			}
					
		
			try {
				heightS = (String) elementAttrs.get("smss-height");
				heightSize = Float.parseFloat(heightS);
			} catch (Exception e) {
				// Specify conditioned on the object type
				if (formType.equals("signature")) {
					heightSize = 53;
				} else if (formType.equals("checkbox")) {
					heightSize = 12;
				} else if (formType.equals("textbox")) {
					heightSize = 55;
				} else {
					heightSize = 53;
				}
				
			}
			
			
			try {
				widthS = (String) elementAttrs.get("smss-width");
				widthSize = Float.parseFloat(widthS);
			} catch (Exception e) {
				// Specify conditioned on the object type
				if (formType.equals("signature")) {
					widthSize = 227;
				} else if (formType.equals("checkbox")) {
					widthSize = 12;
				} else if (formType.equals("textbox")) {
					widthSize = 627;
				} else {
					widthSize = 227;
				}

			}
			
			
			try {
				xShiftS = (String) elementAttrs.get("smss-xshift");
				xShift = Float.parseFloat(xShiftS);
			} catch (Exception e) {
				// Specify conditioned on the object type
				if (formType.equals("signature")) {
					xShift = 0;
				} else if (formType.equals("checkbox")) {
					xShift = 110;
				} else if (formType.equals("textbox")) {
					xShift = 0;
				} else {
					xShift = 0;
				}
			}
			
			
			try {
				yShiftS = (String) elementAttrs.get("smss-yshift");
				yShift = Float.parseFloat(yShiftS);
			} catch (Exception e) {
				// Specify conditioned on the object type
				if (formType.equals("signature")) {
					yShift = - heightSize - 5;
				} else if (formType.equals("checkbox")) {
					yShift = 0;
				} else if (formType.equals("textbox")) {
					yShift = - heightSize - 5;
				} else {
					yShift = 0;
				}
			}
			
			// Convert to PDF pts (pts = px * inch / px * pt / inch)
//			heightSize = 72 * heightSize / 96;
//			widthSize = 72 * widthSize / 96;
//			xShift = 72 * xShift / 96;
//			yShift = 72 * yShift / 96;
			
			heightSize = convertToPts(heightSize);
			widthSize = convertToPts(widthSize);
			xShift = convertToPts(xShift);
			yShift = convertToPts(yShift);

			// Need to determine the tag that we will use
			LLX = pageLocationList.get(index).xCoord;
			LLY = pageLocationList.get(index).yCoord;
			pageNum = pageLocationList.get(index).page;
			
			keyword = (String) elementAttrs.get("smss-value");
			if (keyword.equals("")) {			
				keyword = formType + String.valueOf(index);
			}
			
			//formObjectList.add(new FormObject(new PDRectangle(LLX, LLY, widthSize, heightSize), pageNum, keyword));
			
			formObjectList.add(new FormObject(new PDRectangle(LLX + xShift, LLY + yShift, widthSize, heightSize), pageNum, keyword, formType));
			
			//System.out.println("Attributes in set form: x: " + (LLX + xShift)  + ", y: " + (LLY + yShift)  + ", pageNum: " + pageNum  + ", keyword: " + keyword + ", formType: "  + formType + ", heightSize: " + heightSize + ", widthSize: " + widthSize + ", xshift: " + xShift + ", yshift: " + yShift);
			
			index++;
		}
		return formObjectList;
	}
	
	
	/**
	public static ArrayList<FormObject> setFormObjectLocation(List<String> widthHeightStyle, List<pageLocation> pageLocationList) throws IOException {
		String heightS;
		String widthS;
		String keyword;
		String formType;
		String xShiftS;
		String yShiftS;
		float heightSize;
		float widthSize;
		float xShift;
		float yShift;
		int index = 0;
		float LLX;
		float LLY;
		int pageNum;
		ArrayList<FormObject> formObjectList = new ArrayList<FormObject>();
		
		for (String styleAttr : widthHeightStyle) {
			
			heightS = parseStyleAttr(styleAttr, "height");
			heightSize = Float.parseFloat(heightS);
			widthS = parseStyleAttr(styleAttr, "width");
			widthSize = Float.parseFloat(widthS);
			xShiftS = parseStyleAttr(styleAttr, "xshift");
			yShiftS = parseStyleAttr(styleAttr, "yshift");
			
			if (!xShiftS.equals("")) {
				xShift = Float.parseFloat(xShiftS);
			} else {
				xShift = 0;
			}
			if (!yShiftS.equals("")) {
				yShift = Float.parseFloat(yShiftS);
			} else {
				yShift = 0;
			}
			
			// Convert to PDF pts (pts = px * inch / px * pt / inch)
			heightSize = 72 * heightSize / 96;
			widthSize = 72 * widthSize / 96;
			xShift = 72 * xShift / 96;
			yShift = 72 * yShift / 96;

			// Need to determine the tag that we will use
			formType = parseStyleAttr(styleAttr, "objecttype");
			LLX = pageLocationList.get(index).xCoord;
			LLY = pageLocationList.get(index).yCoord;
			pageNum = pageLocationList.get(index).page;
			keyword = parseStyleAttr(styleAttr, "defaultValue");
			
			if (keyword.equals("")) {			
				keyword = formType + String.valueOf(index);
			}
			
			//formObjectList.add(new FormObject(new PDRectangle(LLX, LLY, widthSize, heightSize), pageNum, keyword));
			
			formObjectList.add(new FormObject(new PDRectangle(LLX + xShift, LLY + yShift, widthSize, heightSize), pageNum, keyword, formType));
			
			index++;
		}
		return formObjectList;
	}
	*/
	
	
	/**
	 * 
	 * Parse height and width from style attribute and adjust size of corresponding rectangle
	 * 
	 * TODO: If we find more elements than signature blocks or vice versa, throw warning
	 * TODO: Generalize to adjust the size of any rectangle field we put in the PDF
	 * 	- pass xShift, yShift in style attr
	 * 	- parse from style attr
	 *  - apply xShift, yShift
	 */
	public static void adjustBlockSizes(List<String> widthHeightStyle, List<RectanglePage> rectPageList) throws IOException {
		String heightS;
		String widthS;
		float heightSize;
		float widthSize;
		int index = 0;
		float LLX;
		float LLY;
		for (String styleAttr : widthHeightStyle) {
			
			heightS = styleAttr.substring(styleAttr.lastIndexOf("height:") + 7).trim();
			heightS = heightS.substring(0, heightS.indexOf(";"));
			heightS = heightS.replace("px", "");
			
			heightSize = Float.parseFloat(heightS);

			widthS = styleAttr.substring(styleAttr.lastIndexOf("width:") + 6).trim();
			widthS = widthS.substring(0, widthS.indexOf(";"));
			widthS = widthS.replace("px", "");
			
			widthSize = Float.parseFloat(widthS);						
			
			// Convert to PDF pts (pts = px * inch / px * pt / inch)

			heightSize = 72 * heightSize / 96;
			widthSize = 72 * widthSize / 96;
			
			LLX = rectPageList.get(index).rect.getLowerLeftX();
			LLY = rectPageList.get(index).rect.getLowerLeftY();
			
			// Reset the rectangle based on new dimensions
			
			// Need a way to ensure that the element actually matches the signature label - maybe we use ids?
			rectPageList.get(index).setRectangle(new PDRectangle(LLX, LLY, widthSize, heightSize));
			
			index++;
		}
	}
	
	/**
	 * 
	 * @param rectPageList
	 * @throws IOException
	 * 
	 * TODO: Consider adding a "shift" argument, or getting this from the style attr
	 * 	- right - apply a shift to put checkbox to right of search term
	 * 	- left - apply a shift to put checkbox to left of search term
	 *  - above - apply shift to put above
	 *  - below - apply shift to put above
	 *  Could also consider passing the x, y shift as px values in the style attr
	 */
	public static void adjustChecboxSizes(List<RectanglePage> rectPageList) throws IOException {
		
		int index = 0;
		float LLX;
		float LLY;
		
		for (RectanglePage rp : rectPageList ) {	
			LLX = rectPageList.get(index).rect.getLowerLeftX();
			LLY = rectPageList.get(index).rect.getLowerLeftY();
			
			rectPageList.get(index).setRectangle(new PDRectangle(LLX + 80, LLY + 40, 9, 9));

			String newKeyword = rectPageList.get(index).getKeyword() + String.valueOf(index);
			rectPageList.get(index).setKeyword(newKeyword);
			
			index++;
		}
	}
	
	
	
	public static void adjustTextboxSizes(List<RectanglePage> rectPageList) throws IOException {
		
		int index = 0;
		float LLX;
		float LLY;
				
		for (RectanglePage rp : rectPageList ) {	
			LLX = rectPageList.get(index).rect.getLowerLeftX();
			LLY = rectPageList.get(index).rect.getLowerLeftY();
			
			rectPageList.get(index).setRectangle(new PDRectangle(LLX, LLY - 5, 470, 40));

			String newKeyword = "Name, Title; Organization";
			rectPageList.get(index).setKeyword(newKeyword);
			
			index++;
		}
		
	}
	
	
	
	/**
	 * 
	 * Add files in a directory as attachments to an existing PDF
	 * 
	 */
	public static void attachFiles(PDDocument document, File filedir) throws IOException {
	      PDEmbeddedFilesNameTreeNode efTree = new PDEmbeddedFilesNameTreeNode();
		  Collection<File> files = FileUtils.listFiles(filedir, null, true);
		  InputStream is;
		  PDComplexFileSpecification fs;
		  PDEmbeddedFile ef;
		  Map efMap = new HashMap();
		  List<PDEmbeddedFile> filesToEmbed = new ArrayList<>();

		  for(File file2 : files){
			  fs = new PDComplexFileSpecification();
			  fs.setFile(file2.getName());
			  is = new FileInputStream(file2.getAbsolutePath());
			  ef = new PDEmbeddedFile(document, is );
			  filesToEmbed.add(ef);
			  
			  //set some of the attributes of the embedded file
			  ef.setSubtype( "test/plain" );
			  ef.setCreationDate( new GregorianCalendar() );
			  fs.setEmbeddedFile( ef );
			  
			  efMap.put( file2.getName(), fs );
		  }
		  
		  efTree.setNames( efMap );
		  PDDocumentNameDictionary names = new PDDocumentNameDictionary( document.getDocumentCatalog() );
		  names.setEmbeddedFiles( efTree );
		  document.getDocumentCatalog().setNames( names );
	}
	
	
	
	public static void addTextField(PDDocument document, FormObject rectPage) throws IOException {
		int pageNum = rectPage.page;
    	PDPage page = document.getPage(pageNum-1);
    	PDRectangle rect = rectPage.rect;
    	PDAcroForm acroForm = document.getDocumentCatalog().getAcroForm();
		if (acroForm == null) {
	        // Add a new AcroForm and add that to the document
			acroForm = new PDAcroForm(document);
		}
		document.getDocumentCatalog().setAcroForm(acroForm);
		
		PDAppearanceStream pdAppearanceStream = new PDAppearanceStream(document);
		
		COSDictionary normalAppearances = new COSDictionary();
		PDAppearanceDictionary pdAppearanceDictionary = new PDAppearanceDictionary();
		pdAppearanceDictionary.setNormalAppearance(new PDAppearanceEntry(normalAppearances));
		pdAppearanceDictionary.setDownAppearance(new PDAppearanceEntry(normalAppearances));
		
		pdAppearanceStream.setBBox(rect);
		normalAppearances.setItem("Yes", pdAppearanceStream);
		
		PDPageContentStream contentStream = new PDPageContentStream(document, page, AppendMode.APPEND, false);
        contentStream.addRect(rect.getLowerLeftX(), rect.getLowerLeftY(), rect.getWidth(), rect.getHeight());
        contentStream.setLineWidth(1);
        contentStream.setNonStrokingColor(Color.WHITE);
        contentStream.setStrokingColor(Color.BLACK);
        contentStream.stroke();
        contentStream.close();
		
        PDTextField textBox = new PDTextField(acroForm);
		acroForm.getFields().add(textBox);
		textBox.setPartialName(rectPage.keyword);
		textBox.setFieldFlags(4);
		textBox.setMultiline(true);
		
		PDFont font = PDType1Font.HELVETICA;
		PDResources resources = new PDResources();
		resources.put(COSName.getPDFName("Helv"), font);
		acroForm.setDefaultResources(resources);
		
		String defaultAppearance = "/Helv 12 Tf 0 0 0 rg";
		textBox.setDefaultAppearance(defaultAppearance);

		List<PDAnnotationWidget> widgets = textBox.getWidgets();
		for (PDAnnotationWidget pdAnnotationWidget : widgets)
		{
		    pdAnnotationWidget.setRectangle(rect);
		    pdAnnotationWidget.setPage(page);
		    page.getAnnotations().add(pdAnnotationWidget);

		    pdAnnotationWidget.setAppearance(pdAppearanceDictionary);
		}
		textBox.setValue(rectPage.getKeyword());
	}
	
	
	
	public static void addCheckbox(PDDocument document, FormObject rectPage) throws IOException {
		int pageNum = rectPage.page;
    	PDPage page = document.getPage(pageNum-1);
    	PDRectangle rect = rectPage.rect;
    	PDAcroForm acroForm = document.getDocumentCatalog().getAcroForm();
		if (acroForm == null) {
	        // Add a new AcroForm and add that to the document
			acroForm = new PDAcroForm(document);
		}
		document.getDocumentCatalog().setAcroForm(acroForm);

		COSDictionary normalAppearances = new COSDictionary();
		PDAppearanceDictionary pdAppearanceDictionary = new PDAppearanceDictionary();
		pdAppearanceDictionary.setNormalAppearance(new PDAppearanceEntry(normalAppearances));
		pdAppearanceDictionary.setDownAppearance(new PDAppearanceEntry(normalAppearances));

		PDAppearanceStream pdAppearanceStream = new PDAppearanceStream(document);
		pdAppearanceStream.setResources(new PDResources());
		try (PDPageContentStream pdPageContentStream = new PDPageContentStream(document, pdAppearanceStream))
		{
			float fontSize = Float.min(rect.getWidth(), rect.getHeight()) - 2;
		    pdPageContentStream.setFont(PDType1Font.ZAPF_DINGBATS, fontSize);
		    pdPageContentStream.beginText();
		    pdPageContentStream.newLineAtOffset(3, 4);
		    pdPageContentStream.showText("\u2714");
		    pdPageContentStream.endText();
		}
		pdAppearanceStream.setBBox(new PDRectangle(rect.getWidth(), rect.getHeight()));
		normalAppearances.setItem("Yes", pdAppearanceStream);

		pdAppearanceStream = new PDAppearanceStream(document);
		pdAppearanceStream.setResources(new PDResources());
		try (PDPageContentStream pdPageContentStream = new PDPageContentStream(document, pdAppearanceStream))
		{
		    pdPageContentStream.setFont(PDType1Font.ZAPF_DINGBATS, 14.5f);
		    pdPageContentStream.beginText();
		    pdPageContentStream.newLineAtOffset(3, 4);
		    pdPageContentStream.showText(" ");
		    pdPageContentStream.endText();
		}
		pdAppearanceStream.setBBox(new PDRectangle(rect.getWidth(), rect.getHeight()));
		normalAppearances.setItem("Off", pdAppearanceStream);
		
		PDPageContentStream contentStream = new PDPageContentStream(document, page, AppendMode.APPEND, false);
        contentStream.addRect(rect.getLowerLeftX(), rect.getLowerLeftY(), rect.getWidth(), rect.getHeight());
        contentStream.setLineWidth(1);
        contentStream.setNonStrokingColor(Color.WHITE);
        contentStream.setStrokingColor(Color.BLACK);
        contentStream.stroke();
        contentStream.close();

		PDCheckBox checkBox = new PDCheckBox(acroForm);
		acroForm.getFields().add(checkBox);
		checkBox.setPartialName(rectPage.keyword);
		checkBox.setFieldFlags(4);

		List<PDAnnotationWidget> widgets = checkBox.getWidgets();
		for (PDAnnotationWidget pdAnnotationWidget : widgets)
		{
		    pdAnnotationWidget.setRectangle(rect);
		    pdAnnotationWidget.setPage(page);
		    page.getAnnotations().add(pdAnnotationWidget);

		    pdAnnotationWidget.setAppearance(pdAppearanceDictionary);
		}
		checkBox.unCheck();
	}
	
	

	public static void addPDFObjects(PDDocument document, ArrayList<FormObject> formObjectList) throws IOException {
		for (FormObject fo : formObjectList ) {
			if (fo.formType.equals("signature")) {
				//System.out.println("Trying to add signature");
			    addSignatureBlock(document, fo);
			} else if (fo.formType.equals("checkbox")) {
				//System.out.println("Trying to add check");
			    addCheckbox(document, fo);
			} else if (fo.formType.equals("textbox")) {
				//System.out.println("Trying to add textbox");
				addTextField(document, fo);
			}
		}
	}
	
}
