package prerna.engine.impl.vector;

import java.util.List;

public class FaissDatabaseCSVRow {

    private String id; // Unique identifier for the row

    private List<? extends Number> embeddings = null; // Placeholder for actual embeddings

    private String source;
    private String modality;
    private String divider;
    private String part;
    private Integer tokens;
    private String content;

    // TODO: revisit how this is stored in db
    private String keywords = "";

    // Modified constructor to accept id as first parameter
    public FaissDatabaseCSVRow(String id, String source, String modality, String divider, String part, Number tokens, String content) {
        this.id = id;
        this.source = source;
        this.modality = modality;
        this.divider = divider;
        this.part = part;
        this.tokens = tokens.intValue();
        this.content = content;
    }

    // Getter and setter for id
    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    // Embeddings methods
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
