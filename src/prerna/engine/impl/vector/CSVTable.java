package prerna.engine.impl.vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.pgvector.PGvector;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;

public class CSVTable {
	

    protected List<CSVRow> rows;
    private IModelEngine keywordEngine = null;
	private int maxKeywords = 12;
	private int percentile = 0;
	
    public CSVTable() {
        this.rows = new ArrayList<>();
    }

    public void addRow(String source, String modality, String divider, String part, int tokens, String content) {
    	CSVRow newRow = new CSVRow(source, modality, divider, part, tokens, content);
        this.rows.add(newRow);
    }
    
    public void addRow(String source, String modality, String divider, String part, String tokens, String content) {
    	CSVRow newRow = new CSVRow(source, modality, divider, part, Double.valueOf(tokens).intValue(), content);
        this.rows.add(newRow);
    }
            
    public List<String> getAllContent() {
        List<String> contents = new ArrayList<>();
        for (CSVRow row : rows) {
            contents.add(row.content);
        }
        return contents;
    }
    
    public List<CSVRow> getRows() {
    	return this.rows;
    }
    
    public void setKeywordEngine(IModelEngine keywordEngine) {
        this.keywordEngine = keywordEngine;
    }
    
    public IModelEngine getKeywordEngine() {
        return this.keywordEngine;
    }
    
    public void generateAndAssignEmbeddings(IModelEngine modelEngine, Insight insight) {
    	List<String> stringsToEmbed = this.getAllContent();
    	
    	if (this.keywordEngine != null) {
    		
    		Map<String, Object> keywordEngineParams = new HashMap<>();
    		keywordEngineParams.put("max_keywords", maxKeywords);
    		keywordEngineParams.put("percentile", percentile);
    		
    		@SuppressWarnings({"unchecked" })
			List<String> keywordsFromChunks = (List<String>) this.keywordEngine.model(stringsToEmbed, insight, keywordEngineParams); 		
    		
    		for (int i = 0; i < this.rows.size(); i++) {
    			String keywordChunk = keywordsFromChunks.get(i);
    			
    			if (keywordChunk != null && !(keywordChunk=keywordChunk.trim()).isEmpty()) {
    				this.rows.get(i).setKeywords(keywordChunk);
    				stringsToEmbed.add(i, keywordChunk);
    			}
    		}
    	}
    	
		EmbeddingsModelEngineResponse output = modelEngine.embeddings(stringsToEmbed, insight, null);
    	
		List<List<Double>> vectors = output.getResponse();

		for (int i = 0; i < this.rows.size(); i++) {
			this.rows.get(i).setEmbeddings(new PGvector(vectors.get(i)));
		}
    }
}
