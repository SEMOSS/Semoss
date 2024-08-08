package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFNotes;
import org.apache.poi.xslf.usermodel.XSLFShape;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFTextParagraph;
import org.apache.poi.xslf.usermodel.XSLFTextShape;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.Constants;

public class PPTProcessor {

	private static final Logger classLogger = LogManager.getLogger(PPTProcessor.class);

	// constructor with file name
	// For every slide get the text shapes
	// index it into a csv
	private String filePath = null;
	private VectorDatabaseCSVWriter writer = null;
	
	/**
	 * 
	 * @param filePath
	 * @param writer
	 */
	public PPTProcessor(String filePath, VectorDatabaseCSVWriter writer) {
		this.filePath = filePath;
		this.writer = writer;
	}
	
	/**
	 * 
	 */
	public void process() {
		FileInputStream is = null;
		XMLSlideShow ppt = null;
		try {
			try {
				is = new FileInputStream(this.filePath);
			} catch (FileNotFoundException e) {
				classLogger.error(Constants.STACKTRACE, e);
				return;
			}

			try {
				ppt = new XMLSlideShow(is);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				return;
			}

			processSlides(ppt);
		} finally {
			if(ppt != null) {
				try {
					ppt.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if(is != null) {
				try {
					is.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	/**
	 * 
	 * @param ppt
	 */
	private void processSlides(XMLSlideShow ppt) {
//      CoreProperties props = ppt.getProperties().getCoreProperties();
//      String title = props.getTitle();
		
		String source = getSource(this.filePath);
        int count = 1;
        for (XSLFSlide slide: ppt.getSlides()) {
        	//System.out.println("Processing Slide..." + count);
        	StringBuilder slideText = new StringBuilder();
        	
        	List<XSLFShape> shapes = slide.getShapes();
        	for (XSLFShape shape: shapes) {
        		if (shape instanceof XSLFTextShape) {
        	        XSLFTextShape textShape = (XSLFTextShape)shape;
        	        String text = textShape.getText();
        	        slideText.append(text);
        	        //System.out.println("Text: " + text);
        		}
        	}
        	
        	// get the notes
        	XSLFNotes mynotes = slide.getNotes();
        	if(mynotes != null) {
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
        	this.writer.writeRow(source, count+"", slideText.toString(), "");
        	
        	//System.out.println("----------------------------");
        	count++;
        }	 
	}	

	/**
	 * 
	 * @param filePath
	 * @return
	 */
	private String getSource(String filePath) {
		String source = null;
		File file = new File(filePath);
		if(file.exists()) {
			source = file.getName();
		}
//		source = Utility.cleanString(source, true);
		return source;
	}
	
}
