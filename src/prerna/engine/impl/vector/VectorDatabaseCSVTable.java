package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.util.flatfile.CsvFileIterator;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
import prerna.query.querystruct.CsvQueryStruct;

public class VectorDatabaseCSVTable {
	
	public static final String SOURCE = "Source";
	public static final String MODALITY = "Modality";
	public static final String DIVIDER = "Divider";
	public static final String PART = "Part";
	public static final String TOKENS = "Tokens";
	public static final String CONTENT = "Content";
	
    public List<VectorDatabaseCSVRow> rows;
    private IModelEngine keywordEngine = null;
	private int maxKeywords = 12;
	private int percentile = 0;
	
    public VectorDatabaseCSVTable() {
        this.rows = new ArrayList<>();
    }

    public void addRow(String source, String modality, String divider, String part, Number tokens, String content) {
    	VectorDatabaseCSVRow newRow = new VectorDatabaseCSVRow(source, modality, divider, part, tokens, content);
        this.rows.add(newRow);
    }
    
    public void addRow(String source, String modality, String divider, String part, String tokens, String content) {
    	VectorDatabaseCSVRow newRow = new VectorDatabaseCSVRow(source, modality, divider, part, Double.valueOf(tokens).intValue(), content);
        this.rows.add(newRow);
    }
            
    public List<String> getAllContent() {
        List<String> contents = new ArrayList<>();
        for (VectorDatabaseCSVRow row : rows) {
            contents.add(row.getContent());
        }
        return contents;
    }
    
    public List<VectorDatabaseCSVRow> getRows() {
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
			this.rows.get(i).setEmbeddings(vectors.get(i));
		}
    }
    
    public static VectorDatabaseCSVTable initCSVTable(File file) throws IOException {
    	VectorDatabaseCSVTable csvTable = new VectorDatabaseCSVTable();
    	
    	final String STR_DT = SemossDataType.STRING.toString();
    	final String INT_DT = SemossDataType.INT.toString();
    	
    	CsvQueryStruct qs = new CsvQueryStruct();
    	qs.setDelimiter(',');
    	qs.setFilePath(file.getAbsolutePath());
    	qs.setSelectorsAndTypes(new String[] {SOURCE, MODALITY, DIVIDER, PART, TOKENS, CONTENT}, 
    			new String[] {STR_DT, STR_DT, STR_DT, STR_DT, INT_DT, STR_DT});
    	
    	CsvFileIterator csvIt = null;
    	try {
    		csvIt = new CsvFileIterator(qs);
        	while(csvIt.hasNext()) {
        		Object[] row = csvIt.next().getValues();
        		csvTable.addRow(
        				(String) row[0],
        				(String) row[1],
        				(String) row[2],
        				(String) row[3],
        				(Number) row[4],
        				(String) row[5]
    				);
        	}
    	} finally {
    		if(csvIt != null) {
    			csvIt.close();
    		}
    	}

		return csvTable;
    }
}
