package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.util.flatfile.CsvFileIterator;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.EmbeddedModelEngine;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
import prerna.query.querystruct.CsvQueryStruct;

public class FaissDatabaseCSVTable {

    public static final String ID = "ID";
    public static final String SOURCE = "Source";
    public static final String MODALITY = "Modality";
    public static final String DIVIDER = "Divider";
    public static final String PART = "Part";
    public static final String TOKENS = "Tokens";
    public static final String CONTENT = "Content";

    public List<FaissDatabaseCSVRow> rows;
    private Map<String, FaissDatabaseCSVRow> idToRow;  // For fast ID lookup
    private EmbeddedModelEngine keywordEngine = null;
    private int maxKeywords = 12;
    private int percentile = 0;
    private File file;

    public FaissDatabaseCSVTable() {
        this.rows = new ArrayList<>();
        this.idToRow = new HashMap<>();
    }

    // Main addRow - supports ID parameter
    public void addRow(String id, String source, String modality, String divider, String part, Number tokens, String content) {
        FaissDatabaseCSVRow newRow = new FaissDatabaseCSVRow(id, source, modality, divider, part, tokens, content);
        this.rows.add(newRow);
        if (id != null) {
            idToRow.put(id, newRow);
        }
    }

    // Convenience addRow overload for all-String row (e.g. from parsing)
    public void addRow(String id, String source, String modality, String divider, String part, String tokens, String content) {
    	FaissDatabaseCSVRow newRow = new FaissDatabaseCSVRow(id, source, modality, divider, part, Double.valueOf(tokens).intValue(), content);
        this.rows.add(newRow);
        if (id != null) {
            idToRow.put(id, newRow);
        }
    }

    public List<String> getAllContent() {
        List<String> contents = new ArrayList<>();
        for (FaissDatabaseCSVRow row : rows) {
            contents.add(row.getContent());
        }
        return contents;
    }

    public List<FaissDatabaseCSVRow> getRows() {
        return this.rows;
    }

    public FaissDatabaseCSVRow getRowById(String id) {
        return this.idToRow.get(id);
    }

    public File getFile() {
        return this.file;
    }

    public void setKeywordEngine(IModelEngine keywordEngine) {
        if (!(keywordEngine instanceof EmbeddedModelEngine)) {
            throw new IllegalArgumentException("Keyword Engine must be of type EmbeddedModelEngine");
        }
        this.keywordEngine = (EmbeddedModelEngine) keywordEngine;
    }

    public EmbeddedModelEngine getKeywordEngine() {
        return this.keywordEngine;
    }

    /**
     * Generates and assigns embeddings to each row, optionally updating with keywords.
     */
    public void generateAndAssignEmbeddings(IModelEngine modelEngine, Insight insight) {
        List<String> stringsToEmbed = this.getAllContent();

        if (this.keywordEngine != null) {
            Map<String, Object> keywordEngineParams = new HashMap<>();
            keywordEngineParams.put("max_keywords", maxKeywords);
            keywordEngineParams.put("percentile", percentile);

            List<String> keywordsFromChunks = this.keywordEngine.keywordExtraction(stringsToEmbed, insight, keywordEngineParams);
            for (int i = 0; i < this.rows.size(); i++) {
                String keywordChunk = keywordsFromChunks.get(i);

                if (keywordChunk != null && !(keywordChunk = keywordChunk.trim()).isEmpty()) {
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

    /**
     * Initializes table from CSV, expects an ID column (first).
     */
    public static FaissDatabaseCSVTable initCSVTable(File file) throws IOException {
        return initCSVTable(file, -1);
    }

    public static FaissDatabaseCSVTable initCSVTable(File file, long limit) throws IOException {
        FaissDatabaseCSVTable csvTable = new FaissDatabaseCSVTable();
        csvTable.file = file;

        final String STR_DT = SemossDataType.STRING.toString();
        final String INT_DT = SemossDataType.INT.toString();

        CsvQueryStruct qs = new CsvQueryStruct();
        qs.setDelimiter(',');
        qs.setFilePath(file.getAbsolutePath());
        qs.setSelectorsAndTypes(
            new String[] {ID, SOURCE, MODALITY, DIVIDER, PART, TOKENS, CONTENT},
            new String[] {STR_DT, STR_DT, STR_DT, STR_DT, STR_DT, INT_DT, STR_DT}
        );
        if (limit > 0) {
            qs.setLimit(limit);
        }

        CsvFileIterator csvIt = null;
        try {
            csvIt = new CsvFileIterator(qs);
            while (csvIt.hasNext()) {
                Object[] row = csvIt.next().getValues();
                csvTable.addRow(
                        (String) row[0], // id
                        (String) row[1], // source
                        (String) row[2], // modality
                        (String) row[3], // divider
                        (String) row[4], // part
                        (Number) row[5], // tokens
                        (String) row[6]  // content
                );
            }
        } finally {
            if (csvIt != null) {
                csvIt.close();
            }
        }

        return csvTable;
    }

    /**
     * Validates the CSV table for required fields being non-empty/non-null and tokens > 0.
     */
    public static boolean validateCSVTable(File file) throws IOException {
        final String STR_DT = SemossDataType.STRING.toString();
        final String INT_DT = SemossDataType.INT.toString();

        CsvQueryStruct qs = new CsvQueryStruct();
        qs.setDelimiter(',');
        qs.setFilePath(file.getAbsolutePath());
        qs.setSelectorsAndTypes(
            new String[] {ID, SOURCE, MODALITY, DIVIDER, PART, TOKENS, CONTENT},
            new String[] {STR_DT, STR_DT, STR_DT, STR_DT, STR_DT, INT_DT, STR_DT}
        );
        qs.setLimit(10);

        CsvFileIterator csvIt = null;
        try {
            csvIt = new CsvFileIterator(qs);
            while (csvIt.hasNext()) {
                Object[] row = csvIt.next().getValues();
                if (
                    row[0] == null || ((String) row[0]).isEmpty()     // ID
                 || row[1] == null || ((String) row[1]).isEmpty()     // Source
                 || row[2] == null || ((String) row[2]).isEmpty()     // Modality
                 || row[3] == null || ((String) row[3]).isEmpty()     // Divider
                 || row[4] == null || ((String) row[4]).isEmpty()     // Part
                 || row[5] == null || ((Number) row[5]).intValue() <= 0 // Tokens
                 || row[6] == null || ((String) row[6]).isEmpty()     // Content
                ) {
                    return false;
                }
            }
        } finally {
            if (csvIt != null) {
                csvIt.close();
            }
        }

        return true;
    }

    /**
     * Extracts the Source column from the CSV.
     */
    public static Set<String> pullSourceColumn(File file) throws IOException {
        Set<String> uniqueSources = new HashSet<>();

        final String STR_DT = SemossDataType.STRING.toString();
        final String INT_DT = SemossDataType.INT.toString();

        CsvQueryStruct qs = new CsvQueryStruct();
        qs.setDelimiter(',');
        qs.setFilePath(file.getAbsolutePath());
        qs.setSelectorsAndTypes(
            new String[] {ID, SOURCE, MODALITY, DIVIDER, PART, TOKENS, CONTENT},
            new String[] {STR_DT, STR_DT, STR_DT, STR_DT, STR_DT, INT_DT, STR_DT}
        );

        CsvFileIterator csvIt = null;
        try {
            csvIt = new CsvFileIterator(qs);
            while (csvIt.hasNext()) {
                Object[] row = csvIt.next().getValues();
                uniqueSources.add((String) row[1]); // Source is now column 1 (with ID as 0)
            }
        } finally {
            if (csvIt != null) {
                csvIt.close();
            }
        }

        return uniqueSources;
    }
}
