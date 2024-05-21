package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.PackagePart;
import org.apache.poi.xwpf.usermodel.ICell;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableCell;
import org.apache.poi.xwpf.usermodel.XWPFTableRow;

import prerna.util.Constants;
import prerna.util.Utility;

public class DocProcessor {

	private static final Logger classLogger = LogManager.getLogger(DocProcessor.class);

	// constructor with file name
	// For every slide get the text shapes
	// index it into a csv
	private String filePath = null;
	private CSVWriter writer = null;

	public DocProcessor(String filePath, CSVWriter writer) {
		this.filePath = filePath;
		this.writer = writer;
	}

	public void process() {
		FileInputStream is = null;
		XWPFDocument document= null;
		// also process the tables
		// also process embedded documents
		try {
			try {
				is = new FileInputStream(filePath);
			} catch (FileNotFoundException e) {
				classLogger.error(Constants.STACKTRACE, e);
				return;
			}
			
			try {
				document = new XWPFDocument(is);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				return;
			}
			
			processParagraphs(document);
			processTables(document);
			processEmbeds(document);
		} finally {
			if(document != null) {
				try {
					document.close();
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
	 * @param document
	 */
	private void processParagraphs(XWPFDocument document) {
		int count = 1;
		int pageNo = 1;
		String source = getSource(this.filePath);

		for (XWPFParagraph paragraph : document.getParagraphs()) {
			String text = paragraph.getText();
			if (text != null) {
				this.writer.writeRow(source, count + "", text, pageNo + "");
				//System.err.println(text);
			}
			if (paragraph.isPageBreak()) {
				pageNo++;
			}
			count++;
		}
		
	}

	/**
	 * 
	 * @param document
	 */
	private void processTables(XWPFDocument document) {
		int count = 1;
		int pageNo = 1;
		String source = getSource(this.filePath);
		
		String [] headers = null;
		String [] values = null;
		boolean headerProcessed = false;

		for (XWPFTable table : document.getTables()) {
			List <XWPFTableRow> rows = table.getRows();
			for(int rowIndex = 0;rowIndex < rows.size();rowIndex++)
			{
				XWPFTableRow row = table.getRow(rowIndex);
				List <ICell> cells = row.getTableICells();
				String [] processor = new String[cells.size()];
				for(int cellIndex = 0;cellIndex < cells.size();cellIndex++)
				{
					ICell thisCell = cells.get(cellIndex);
					if(thisCell instanceof XWPFTableCell)
					{
						processor[cellIndex] = ((XWPFTableCell)thisCell).getText();
					}
					//System.err.print("|" + processor[cellIndex]);
				}
				//System.out.println("--------------");
				if(!headerProcessed)
				{
					headers = processor;
					headerProcessed = true;
				}
				else
				{
					values = processor;
					StringBuilder rowOut = getRow(headers, values);
					this.writer.writeRow(source, count + "", rowOut+"", pageNo + "");
				}
			}
			//System.err.println("=========");
			headerProcessed = false;
			count++;
		}
	}
	
	/**
	 * 
	 * @param headers
	 * @param values
	 * @return
	 */
	private StringBuilder getRow(String [] headers, String [] values) {
		StringBuilder builder = new StringBuilder();
		for(int valIndex = 0;valIndex < values.length; valIndex++) {
			String header = null;
			if(valIndex < headers.length) {
				header = headers[valIndex];
			} else {
				header = headers[headers.length - 1];
			}
			
			builder.append(header + "=" + values[valIndex]).append(" ");
		}
		return builder;
	}
	
	/**
	 * 
	 * @param document
	 */
	private void processEmbeds(XWPFDocument document) {
		try {
			List <PackagePart> embeds = document.getAllEmbeddedParts();
			for(int embedIndex = 0;embedIndex < embeds.size();embedIndex++)
			{
				PackagePart embed = embeds.get(embedIndex);
				System.err.println(embed.getContentType());
				System.err.println(embed.getContentTypeDetails().getParameterKeys());
				System.err.println(embed);
			}
		} catch (OpenXML4JException e) {
			e.printStackTrace();
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
		source = Utility.cleanString(source, true);
		return source;
	}
	
//	public static void main(String[] args) throws Exception 
//	{
//
//		DocProcessor dp = new DocProcessor("c:/temp/ABACUS_ADP_FY24.docx", null);
//		dp.process();
//	}
}
