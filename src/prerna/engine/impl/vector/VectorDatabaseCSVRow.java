package prerna.engine.impl.vector;

import java.util.List;

public class VectorDatabaseCSVRow {
	
	private List<? extends Number> embeddings = null; // This could be a placeholder or identifier for actual embeddings
	private String source;
	private String modality;
	private String divider;
	private String part;
	private Integer tokens;
	private String content;
	
	// TODO: revisit how this is stored in db
	private String keywords = "";

    public VectorDatabaseCSVRow(String source, String modality, String divider, String part, int tokens, String content) {
        // Initially, embedding might not be set
        this.source = source;
        this.modality = modality;
        this.divider = divider;
        this.part = part;
        this.tokens = tokens;
        this.content = content;
    }

    // Method to update the embeddings for a row
    public void setEmbeddings(List<? extends Number> list) {
        this.embeddings = list;
    }
    
    public List<? extends Number> getEmbeddings() {
        return this.embeddings;
    }
    
    public String getSource() {
    	return this.source;
    }
    
    public String getModality() {
    	return this.modality;
    }
    
    public String getDivider() {
    	return this.divider;
    }
    
    public String getPart() {
    	return this.part;
    }

    public Integer getTokens() {
    	return this.tokens;
    }
    
    public String getContent() {
    	return this.content;
    }
    
    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }
    
    public String getKeywords() {
        return this.keywords;
    }
}
