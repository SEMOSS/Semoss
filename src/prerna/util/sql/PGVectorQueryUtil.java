package prerna.util.sql;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.engine.impl.vector.PGVectorDatabaseEngine;
import prerna.engine.impl.vector.VectorDatabaseCSVTable;
import prerna.engine.impl.vector.metadata.VectorDatabaseMetadataCSVTable;
import prerna.util.Constants;

public class PGVectorQueryUtil extends PostgresQueryUtil {
	
	private static final Logger classLogger = LogManager.getLogger(PGVectorQueryUtil.class);

	public PGVectorQueryUtil() {
		super();
	}

	public PGVectorQueryUtil(String connectionUrl, String username, String password) {
		super();
	}

	public String createEmbeddingsTable(String table) {
		return "DO $$\n" +
	                "DECLARE\n" +
	                "    tbl_name text := '" + table + "';\n" +
	                "BEGIN\n" +
	                "    IF NOT EXISTS (\n" +
	                "        SELECT 1\n" +
	                "        FROM information_schema.tables \n" +
	                "        WHERE table_schema = current_schema() \n" +
	                "          AND table_name = tbl_name\n" +
	                "    ) THEN\n" +
	                "        EXECUTE format('CREATE TABLE %I (\n" +
	                "            ID SERIAL PRIMARY KEY,\n" +
	                "            EMBEDDING vector,\n" +
	                "            SOURCE text,\n" +
	                "            MODALITY text,\n" +
	                "            DIVIDER text,\n" +
	                "            PART text,\n" +
	                "            TOKENS text,\n" +
	                "            CONTENT text\n" +
	                "        )', tbl_name);\n" +
	                "    END IF;\n" +
	                "END $$;";
	}
	
	public String createEmbeddingsMetadataTable(String table) {
		return "DO $$\n" +
	                "DECLARE\n" +
	                "    tbl_name text := '" + table + "';\n" +
	                "BEGIN\n" +
	                "    IF NOT EXISTS (\n" +
	                "        SELECT 1\n" +
	                "        FROM information_schema.tables \n" +
	                "        WHERE table_schema = current_schema() \n" +
	                "          AND table_name = tbl_name\n" +
	                "    ) THEN\n" +
	                "        EXECUTE format('CREATE TABLE %I (\n" +
	                "            ID INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,\n" +
	                "            SOURCE TEXT,\n" +
	                "            ATTRIBUTE TEXT,\n" +
	                "            STR_VALUE TEXT,\n" +
	                "            INT_VALUE INTEGER,\n" +
	                "            NUM_VALUE NUMERIC(18,4),\n" +
	                "            BOOL_VALUE BOOLEAN,\n" +
	                "            DATE_VAL DATE,\n" +
	                "            TIMESTAMP_VAL TIMESTAMP\n" +
	                "        )', tbl_name);\n" +
	                "    END IF;\n" +
	                "END $$;";
	}

	public String addVectorExtension() {
		return "CREATE EXTENSION IF NOT EXISTS vector;";
	}
	
	public void createOWL(PGVectorDatabaseEngine engine, String embeddingsTable, String metadataTable) {
		try(WriteOWLEngine writer = engine.getOWLEngineFactory().getWriteOWL()) {
			writer.createEmptyOWLFile();
			
			writer.addConcept(embeddingsTable);
			writer.addProp(embeddingsTable, "ID", "IDENTITY");
			writer.addProp(embeddingsTable, VectorDatabaseCSVTable.SOURCE, "TEXT");
			writer.addProp(embeddingsTable, VectorDatabaseCSVTable.MODALITY, "TEXT");
			writer.addProp(embeddingsTable, VectorDatabaseCSVTable.DIVIDER, "TEXT");
			writer.addProp(embeddingsTable, VectorDatabaseCSVTable.PART, "TEXT");
			writer.addProp(embeddingsTable, VectorDatabaseCSVTable.TOKENS, "INTEGER");
			writer.addProp(embeddingsTable, VectorDatabaseCSVTable.CONTENT, "TEXT");
			writer.addProp(embeddingsTable, "EMBEDDING", "VECTOR");

			writer.addConcept(metadataTable);
			writer.addProp(metadataTable, "ID", "IDENTITY");
			writer.addProp(metadataTable, VectorDatabaseMetadataCSVTable.SOURCE, "TEXT");
			writer.addProp(metadataTable, VectorDatabaseMetadataCSVTable.ATTRIBUTE, "TEXT");
			writer.addProp(metadataTable, VectorDatabaseMetadataCSVTable.STR_VALUE, "TEXT");
			writer.addProp(metadataTable, VectorDatabaseMetadataCSVTable.INT_VALUE, "INTEGER");
			writer.addProp(metadataTable, VectorDatabaseMetadataCSVTable.NUM_VALUE, "NUMERIC(18,4)");
			writer.addProp(metadataTable, VectorDatabaseMetadataCSVTable.BOOL_VALUE, "BOOLEAN");
			writer.addProp(metadataTable, VectorDatabaseMetadataCSVTable.DATE_VAL, "DATE");
			writer.addProp(metadataTable, VectorDatabaseMetadataCSVTable.TIMESTAMP_VAL, "TIMESTAMP");

			writer.addRelation(embeddingsTable, metadataTable, 
					embeddingsTable+"."+VectorDatabaseCSVTable.SOURCE+"."+metadataTable+"."+VectorDatabaseMetadataCSVTable.SOURCE);
			
			writer.commit();
			writer.export();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (InterruptedException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

}
