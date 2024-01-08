package prerna.engine.impl.vector;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.text.StringEscapeUtils;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.SentenceUtils;
import edu.stanford.nlp.process.DocumentPreprocessor;
import prerna.ds.py.PyUtils;
import prerna.ds.py.TCPPyTranslator;
import prerna.reactor.frame.gaas.processors.CSVWriter;

public class VectorDatabaseCSVWriter extends CSVWriter {

	String faissDbVarName;
	TCPPyTranslator vectorPyt;
	int rowsCreated;
	
	public VectorDatabaseCSVWriter(String fileName) {
		super(fileName);
	}
	
	public void setFaissDbVarName(String faissDbVarName) {
		this.faissDbVarName = faissDbVarName;
	}
	
	public void setPyTranslator(TCPPyTranslator vectorPyt) {
		this.vectorPyt = vectorPyt;
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
	
	/**
	 * 
	 * @param content
	 * @return
	 */
	protected List<String []> breakSentences(String content) {
		List<String []> blockInformation = new ArrayList<String []>();
		
		Reader reader = new StringReader(content);
		DocumentPreprocessor dp = new DocumentPreprocessor(reader);
		List<String> sentenceList = new ArrayList<String>();
		
		for (List<HasWord> sentence : dp) {
			// SentenceUtils not Sentence
			String sentenceString = SentenceUtils.listToString(sentence);
	        sentenceString = sentenceString.replaceAll("\\s+", " ");

	        // Remove non-printable characters
	        sentenceString = sentenceString.replaceAll("[^\\x20-\\x7E]", "");

	        // Unescape HTML entities
	        sentenceString = StringEscapeUtils.unescapeHtml4(sentenceString);
	        
	        // Remove underscores (especially repeated ones)
	        sentenceString = sentenceString.replaceAll("_+", "");
	        
			sentenceList.add(sentenceString);
		}
		
		List<Number> tokensInSentences = (List<Number>) this.vectorPyt.runScript(this.faissDbVarName + ".getTokensInSentences(sentences = "+ PyUtils.determineStringType(sentenceList) + ")");

		
        double totalTokensInContent = tokensInSentences.stream()
                .mapToDouble(Number::doubleValue)
                .sum();
		
		//if there is only 30 chars, return empty
		if(totalTokensInContent < this.minContentLength) {
			return blockInformation;
		}
		
		//if its less than the contentlength, add it to the block and return
		if(totalTokensInContent <= this.contentLength) {
			blockInformation.add(new String [] {content, String.valueOf(totalTokensInContent)});
		}
		
		else {
			blockInformation = createChunks(sentenceList, tokensInSentences, blockInformation);
		}
		return blockInformation;
	}

	public List<String []> createChunks(List<String> sentences, List<Number> tokensInSentences, List<String []> chunks) {
		StringBuilder currentChunk = new StringBuilder();
		Double tokensInCurrentChunk = 0.0;
		
		for (int i = 0 ; i <sentences.size(); i++) {
			String sentence = sentences.get(i);
			Double numTokensInSentence = tokensInSentences.get(i).doubleValue();
			
			if (tokensInCurrentChunk + numTokensInSentence <= contentLength) {
				if (currentChunk.length() > 0) {
					currentChunk.append(" "); // Add space for sentence separation
				}
				currentChunk.append(sentence);
				tokensInCurrentChunk += numTokensInSentence;
			} else {
				String chunk = currentChunk.toString();
				chunks.add(new String [] {chunk, String.valueOf(tokensInCurrentChunk)});
				
				// start new chunk
				String overlap = "";
				if (overlapLength > 0) {
					overlap = getOverlap(chunk, overlapLength);
				}
				
				currentChunk = new StringBuilder(overlap).append(" ").append(sentence);
				tokensInCurrentChunk = overlapLength + numTokensInSentence;
			}
		}

		if (currentChunk.length() > 0) {
			chunks.add(new String [] {currentChunk.toString(), String.valueOf(tokensInCurrentChunk)});
		}

		return chunks;
	}
	
	protected String getOverlap(String chunk, int overlapLength) {
		StringBuilder overlapStringScript = new StringBuilder(faissDbVarName);
		
		overlapStringScript.append(".getOverlapTokensAsString(")
						   .append("chunk = ").append(PyUtils.determineStringType(chunk))
						   .append(",")
						   .append("overlapLength = ").append(PyUtils.determineStringType(overlapLength))
						   .append(")");
		
		Object overlapString = this.vectorPyt.runScript(overlapStringScript.toString());
		return overlapString + "";
	}
}
