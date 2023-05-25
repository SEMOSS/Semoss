package prerna.sablecc2.reactor.frame.gaas.processors;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.pdfbox.cos.COSDocument;
import org.apache.pdfbox.io.RandomAccessFile;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import prerna.util.Utility;

public class PDFProcessor {

	// constructor with file name
	// For every slide get the text shapes
	// index it into a csv
	String fileName = null;
	CSVWriter writer = null;
	
	public PDFProcessor(String fileName, CSVWriter writer)
	{
		this.fileName = fileName;
		this.writer = writer;
	}
	
	public void process()
	{
		try {
			File f = new File(fileName);
			PDFParser parser = new PDFParser(new RandomAccessFile(f, "r"));
			parser.parse();
			
			COSDocument cosDoc = parser.getDocument();
			String source = getSource(fileName);
			PDFTextStripper pdfStripper = new PDFTextStripper();
			StringBuffer row = new StringBuffer();
			PDDocument pdDoc = new PDDocument(cosDoc);
			int totalPages = pdDoc.getNumberOfPages();
			for(int pageIndex = 0;pageIndex < totalPages;pageIndex++)
			{
				//System.out.println("Processing Page " + pageIndex);
				pdfStripper.setStartPage(pageIndex);
				pdfStripper.setEndPage(pageIndex);
				String parsedText = pdfStripper.getText(pdDoc);
				writer.writeRow(source, pageIndex+"", parsedText, "");
			}
			cosDoc.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}	
	
	private String getSource(String fileName)
	{
		String source = null;
		File file = new File(fileName);
		if(file.exists())
			source = file.getName();
				
		source = Utility.cleanString(source, true);
		
		return source;

	}

}
