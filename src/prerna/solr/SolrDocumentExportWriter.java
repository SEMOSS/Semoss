package prerna.solr;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class SolrDocumentExportWriter {

	private FileWriter writer;
	private BufferedWriter buffer;
	
	public SolrDocumentExportWriter(File file) throws IOException{
		this.writer = new FileWriter(file.getAbsoluteFile(), true);
		this.buffer = new BufferedWriter(writer);
	}
	
	public void writeSolrDocument(File file, String id, Map<String, Object> fieldData) throws IOException {
		buffer.write("<SolrInputDocument>");			
		buffer.newLine();
		buffer.write("id : " + id);
		buffer.newLine();
		
		for (String field : fieldData.keySet()) {
			Object fieldVal = fieldData.get(field);
			if (fieldVal instanceof Collection){
				@SuppressWarnings({ "unchecked", "rawtypes" })
				Iterator<Object> iterator = ((Collection) fieldVal).iterator();
				while(iterator.hasNext()){
					buffer.write(field + " : " + iterator.next());
					buffer.newLine();
				}
			} else {
				buffer.write(field + " : " + fieldVal);
				buffer.newLine();
			}
		}
		buffer.write("</SolrInputDocument>");
		buffer.newLine();
	}
	
	public void closeExport() {
		try {
			if(buffer != null) {
				buffer.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
