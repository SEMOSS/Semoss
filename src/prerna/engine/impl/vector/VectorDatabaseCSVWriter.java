package prerna.engine.impl.vector;

import prerna.reactor.frame.gaas.processors.CSVWriter;

public class VectorDatabaseCSVWriter extends CSVWriter {

	int rowsCreated;
	
	public VectorDatabaseCSVWriter(String fileName) {
		super(fileName);
	}
	
	public int getRowsInCsv() {
		return this.rowsCreated;
	}

	@Override
	protected void writeHeader() {
		StringBuffer row = new StringBuffer();
		row.append("Source").append(",")
		.append("Modality").append(",")
		.append("Divider").append(",")
		.append("Part").append(",")
		.append("Content")
		.append("\r\n");
		pw.print(row + "");
		
		// this should always be the first row
		this.rowsCreated = 1;
	}

	/**
	 * divider is page number or slide number etc. 
	 * @param source
	 * @param divider
	 * @param content
	 * @param misc
	 */
	@Override
	public void writeRow(String source, String divider, String content, String misc)
	{
		// tries to see if text is > token length
		// uses spacy to break this
		// gets the parts and then
		// takes this row and writes it
//		List<String []> contentBlocks = breakSentences(content);
//		
//		for(int contentIndex = 0;contentIndex < contentBlocks.size();contentIndex++) {
//			String thisBlock = contentBlocks.get(contentIndex)[0];
//			String numTokensInBlock = contentBlocks.get(contentIndex)[1];
//			
//			if(thisBlock.length() >= minContentLength) {
//				//System.err.println(contentIndex + " <> " + contentBlocks.get(contentIndex));
//				StringBuilder row = new StringBuilder();
//				row.append("\"").append(cleanString(source)).append("\"").append(",")
//				.append("\"").append(cleanString(divider)).append("\"").append(",")
//				.append("\"").append(contentIndex).append("\"").append(",")
//				.append("\"").append(numTokensInBlock).append("\"").append(",")
//				.append("\"").append(cleanString(thisBlock)).append("\"")
//				.append("\r\n");
//				//System.out.println(row);
//				pw.print(row+"");
//				//pw.print(separator);
//				pw.flush();
//				
//				rowsCreated += 1;
//			}
//		}
		
		StringBuilder row = new StringBuilder();
		row.append("\"").append(cleanString(source)).append("\"").append(",")
		.append("\"").append("text").append("\"").append(",")
		.append("\"").append(cleanString(divider)).append("\"").append(",")
		.append("\"").append(0).append("\"").append(",")
		.append("\"").append(cleanString(content)).append("\"")
		.append("\r\n");
		//System.out.println(row);
		pw.print(row+"");
		//pw.print(separator);
		pw.flush();
		
		rowsCreated += 1;
	}
}
