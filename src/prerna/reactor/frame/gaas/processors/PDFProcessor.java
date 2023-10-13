package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

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
		PDDocument pdDoc = null;
		try {
			File f = new File(fileName);
			try {
				String source = getSource(fileName);
				PDFTextStripper pdfStripper = new PDFTextStripper();
				pdDoc = PDDocument.load(f);
				int totalPages = pdDoc.getNumberOfPages();
				for(int pageIndex = 0;pageIndex < totalPages;pageIndex++)
				{
					pdfStripper.setStartPage(pageIndex);
					pdfStripper.setEndPage(pageIndex);
					String parsedText = pdfStripper.getText(pdDoc);
					writer.writeRow(source, pageIndex+"", parsedText, "");
				}
				
	            pdDoc.close(); // Close PDDocument when done
				} catch (FileNotFoundException e) {
					pdDoc.close();
					e.printStackTrace();
				} 	
			} catch (IOException e) {
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
