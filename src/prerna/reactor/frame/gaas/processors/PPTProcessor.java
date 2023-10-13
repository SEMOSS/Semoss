package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.poi.ooxml.POIXMLProperties.CoreProperties;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFNotes;
import org.apache.poi.xslf.usermodel.XSLFShape;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFTextParagraph;
import org.apache.poi.xslf.usermodel.XSLFTextShape;

import prerna.util.Utility;

public class PPTProcessor {

	// constructor with file name
	// For every slide get the text shapes
	// index it into a csv
	String fileName = null;
	CSVWriter writer = null;
	
	public PPTProcessor(String fileName, CSVWriter writer)
	{
		this.fileName = fileName;
		this.writer = writer;
	}
	
	public void process()
	{
		FileInputStream inputStream;
		
		try {
			inputStream = new FileInputStream(fileName);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return;
		}
		
		XMLSlideShow ppt;
		
		try {
			ppt = new XMLSlideShow(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		processSlides(ppt);
	}
	
	public  void processSlides(XMLSlideShow ppt) 
	{
        CoreProperties props = ppt.getProperties().getCoreProperties();
        String title = props.getTitle();
		String source = getSource(fileName);

        
        int count = 1;
        for (XSLFSlide slide: ppt.getSlides()) {
        	//System.out.println("Processing Slide..." + count);
        	StringBuilder slideText = new StringBuilder();
        	
        	List<XSLFShape> shapes = slide.getShapes();
        	for (XSLFShape shape: shapes) 
        	{
        		if (shape instanceof XSLFTextShape) 
        		{
        	        XSLFTextShape textShape = (XSLFTextShape)shape;
        	        String text = textShape.getText();
        	        slideText.append(text);
        	        //System.out.println("Text: " + text);
        		}
        	}
        	
        	// get the notes
        	XSLFNotes mynotes = slide.getNotes();
        	if(mynotes != null)
        	{
	            for (XSLFShape shape : mynotes) {
	                if (shape instanceof XSLFTextShape) {
	                    XSLFTextShape txShape = (XSLFTextShape) shape;
	                    for (XSLFTextParagraph xslfParagraph : txShape.getTextParagraphs()) {
	                    	String text = xslfParagraph.getText();
	                        //System.out.println(text);
	                        slideText.append(text);
	                    }
	                }
	            }
        	}
        	writer.writeRow(source, count+"", slideText.toString(), "");
        	
        	
        	//System.out.println("----------------------------");
        	count++;
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
