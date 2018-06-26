//package prerna.solr;
//
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.util.Collection;
//import java.util.Iterator;
//import java.util.Map;
//
//import org.apache.solr.common.SolrDocument;
//import org.apache.solr.common.SolrDocumentList;
//
//public class SolrDocumentExportWriter {
//
//	/*
//	 * This is a class used to write the metadata around the insight
//	 * 
//	 * There are 2 different ways to export this information:
//	 * 1) using the solr objects
//	 * 2) using the unique id that associated with the insight that is stored in solr 
//	 * 			and a map containing the metadata
//	 * 
//	 * The reason both are exposed is based on the flow of execution.  Currently, this code
//	 * is executed when caching occurs on the BE.  Caching is called when a user executes an
//	 * existing insight and when a user saves a "drag and drop csv" insight.
//	 * 1) When an existing insight is run and caching occurs, the execution will occur passing in
//	 * the solr objects
//	 * 2) When saving a "drag and drop csv" insight, caching needs to occur right away since the data 
//	 * is static.  Since this is occurring during the save process, we already have the unique id and
//	 * the map of the attributes to save the data.. thus instead of adding to solr and then querying solr
//	 * for the solr document, we are just passing in the attributes directly.
//	 * 
//	 */
//	
//	
//	private FileWriter writer;
//	private BufferedWriter buffer;
//	
//	/**
//	 * Constructor for the class
//	 * @param file					Constructor takes in a File where the solr document will be written
//	 * @throws IOException
//	 */
//	public SolrDocumentExportWriter(File file) throws IOException{
//		this.writer = new FileWriter(file.getAbsoluteFile(), true);
//		this.buffer = new BufferedWriter(writer);
//	}
//	
//	
//	/**
//	 * Loop through a list of solr documents and writes them to the file defined in the constructor
//	 * @param docs						The list of solr documents
//	 * @throws IOException
//	 */
//	public void writeSolrDocument(SolrDocumentList docs) throws IOException {
//		// loop through every SolrDocument in the SolrDocumentList
//		for(SolrDocument doc : docs) {
//			writeSolrDocument(doc);
//		}
//	}
//	
//	/**
//	 * Take the solr document and write it to the file
//	 * @param doc						The SolrDocument to write
//	 * @throws IOException
//	 */
//	public void writeSolrDocument(SolrDocument doc) throws IOException {
//		Map<String, Object> fieldData = doc.getFieldValueMap();
//		buffer.write("<SolrInputDocument>");			
//		buffer.newLine();
//		// note that the order of the attributes doesn't matter
//		writeFieldData(fieldData);
//		buffer.write("</SolrInputDocument>");
//		buffer.newLine();
//	}
//	
//	/**
//	 * Take in the solr id and the map containing the insight metadata attributes to write
//	 * @param id						The unique insight id to store in solr
//	 * @param fieldData					The metadata to store 
//	 * @throws IOException
//	 */
//	public void writeSolrDocument(String id, Map<String, Object> fieldData) throws IOException {
//		buffer.write("<SolrInputDocument>");			
//		buffer.newLine();
//		buffer.write("id : " + id);
//		buffer.newLine();
//		// note that the order of the attributes doesn't matter
//		writeFieldData(fieldData);
//		buffer.write("</SolrInputDocument>");
//		buffer.newLine();
//	}
//	
//	/**
//	 * Writes the field data map into the file
//	 * @param fieldData					The metadata to store 
//	 * @throws IOException
//	 */
//	private void writeFieldData(Map<String, Object> fieldData) throws IOException {
//		for (String field : fieldData.keySet()) {
//			Object fieldVal = fieldData.get(field);
//			if (fieldVal instanceof Collection){
//				Iterator<Object> iterator = ((Collection) fieldVal).iterator();
//				while(iterator.hasNext()){
//					buffer.write(field + " : " + iterator.next());
//					buffer.newLine();
//				}
//			} else {
//				buffer.write(field + " : " + fieldVal);
//				buffer.newLine();
//			}
//		}
//	}
//	
//	/**
//	 * Close the stream to the file
//	 */
//	public void closeExport() {
//		try {
//			if(buffer != null) {
//				buffer.close();
//			}
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
//}
