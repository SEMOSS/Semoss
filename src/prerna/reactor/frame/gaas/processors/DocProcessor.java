package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.extractor.WordExtractor;
import org.apache.poi.hwpf.usermodel.Table;
import org.apache.poi.hwpf.usermodel.TableCell;
import org.apache.poi.hwpf.usermodel.TableIterator;
import org.apache.poi.hwpf.usermodel.TableRow;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.PackagePart;
import org.apache.poi.xwpf.usermodel.ICell;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableCell;
import org.apache.poi.xwpf.usermodel.XWPFTableRow;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.Constants;

public class DocProcessor {

	private static final Logger classLogger = LogManager.getLogger(DocProcessor.class);

	// constructor with file name
	// For every slide get the text shapes
	// index it into a csv
	private String filePath = null;
	private VectorDatabaseCSVWriter writer = null;

	public DocProcessor(String filePath, VectorDatabaseCSVWriter writer) {
		this.filePath = filePath;
		this.writer = writer;
	}


	public void process(String filetype) {
		FileInputStream is = null;
		Object document = null; // Use Object to handle both types

		try {
			is = new FileInputStream(filePath);

			// Check the file extension to determine which document type to process
			if (filetype.equals("doc")) {
				document = new HWPFDocument(is);
				processParagraphs((HWPFDocument) document);
				processTables((HWPFDocument) document);
			} else {
				document = new XWPFDocument(is);
				processParagraphs((XWPFDocument) document);
				processTables((XWPFDocument) document);
				processEmbeds((XWPFDocument) document);
			}
		} catch (FileNotFoundException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			closeDocument(document);
			closeInputStream(is);
		}
	}

	private void closeDocument(Object document) {
		if (document != null) {
			if (document instanceof XWPFDocument) {
				try {
					((XWPFDocument) document).close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			} else if (document instanceof HWPFDocument) {
				try {
					((HWPFDocument) document).close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	

	private void closeInputStream(FileInputStream is) {
		if (is != null) {
			try {
				is.close();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
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
			classLogger.error(Constants.STACKTRACE, e);
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
	
//	public static void main(String[] args) throws Exception 
//	{
//
//		DocProcessor dp = new DocProcessor("c:/temp/ABACUS_ADP_FY24.docx", null);
//		dp.process();
//	}


	private void processParagraphs(HWPFDocument document) throws IOException {
		int count = 1;
		int pageNo = 1;
		String source = getSource(this.filePath);

		try (WordExtractor extractor = new WordExtractor(document)) {
			String[] paragraphs = extractor.getParagraphText();

			for (String paragraph : paragraphs) {
				if (paragraph != null && !paragraph.trim().isEmpty()) {
					boolean isPageBreak = paragraph.contains("\f");
					if (isPageBreak) {
						pageNo++;
					}

					this.writer.writeRow(source, String.valueOf(count), paragraph, String.valueOf(pageNo));
				}

				count++;
			}
		} catch (IOException e) {
			classLogger.error("Error extracting paragraphs from document", e);
			throw e;
		}
	}
	private void processTables(HWPFDocument document) {
		int count = 1;
		int pageNo = 1;
		String source = getSource(this.filePath);

		// Use TableIterator to go through tables
		TableIterator tableIterator = new TableIterator(document.getRange());

		while (tableIterator.hasNext()) {
			Table table = tableIterator.next();

			String[] headers = null;
			boolean headerProcessed = false;

			for (int rowIndex = 0; rowIndex < table.numRows(); rowIndex++) {
				TableRow row = table.getRow(rowIndex);
				String[] processor = new String[row.numCells()];

				for (int cellIndex = 0; cellIndex < row.numCells(); cellIndex++) {
					TableCell cell = row.getCell(cellIndex);
					processor[cellIndex] = cell.text(); // Use text() to get cell content
				}

				if (!headerProcessed) {
					headers = processor; // First row as headers
					headerProcessed = true;
				} else {
					StringBuilder rowOut = getRow(headers, processor);
					this.writer.writeRow(source, String.valueOf(count), rowOut.toString(), String.valueOf(pageNo));
				}
			}
			count++;
		}
	}

}
