package prerna.util.sql;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.engine.impl.vector.PGVectorDatabaseEngine;
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
		return "CREATE TABLE IF NOT EXISTS "+table+"("
				+ "ID INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY, "
				+ "EMBEDDING VECTOR, "
				+ "SOURCE TEXT, "
				+ "MODALITY TEXT, "
				+ "DIVIDER TEXT, "
				+ "PART TEXT, "
				+ "TOKENS INTEGER, "
				+ "CONTENT TEXT "
				+ ");";
	}
	
	public String createEmbeddingsMetadataTable(String table) {
		return "CREATE TABLE IF NOT EXISTS "+table+"("
				+ "ID INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY, "
				+ "SOURCE TEXT, "
				+ "ATTRIBUTE TEXT, "
				+ "STR_VALUE TEXT, "
				+ "INT_VALUE INTEGER, "
				+ "NUM_VALUE NUMERIC(18,4), "
				+ "BOOL_VALUE BOOLEAN, "
				+ "DATE_VAL DATE, "
				+ "TIMESTAMP_VAL TIMESTAMP "
				+ ");";
	}

	public String addVectorExtension() {
		return "CREATE EXTENSION IF NOT EXISTS vector;";
	}
	
	public void createOWL(PGVectorDatabaseEngine engine, String embeddingsTable, String metadataTable) {
		try(WriteOWLEngine writer = engine.getOWLEngineFactory().getWriteOWL()) {
			writer.createEmptyOWLFile();
			
			writer.addConcept(embeddingsTable);
			writer.addProp(embeddingsTable, "ID", "IDENTITY");
			writer.addProp(embeddingsTable, "SOURCE", "TEXT");
			writer.addProp(embeddingsTable, "MODALITY", "TEXT");
			writer.addProp(embeddingsTable, "DIVIDER", "TEXT");
			writer.addProp(embeddingsTable, "PART", "TEXT");
			writer.addProp(embeddingsTable, "TOKENS", "INTEGER");
			writer.addProp(embeddingsTable, "CONTENT", "TEXT");
			writer.addProp(embeddingsTable, "EMBEDDING", "VECTOR");

			writer.addConcept(metadataTable);
			writer.addProp(metadataTable, "ID", "IDENTITY");
			writer.addProp(metadataTable, "SOURCE", "TEXT");
			writer.addProp(metadataTable, "ATTRIBUTE", "TEXT");
			writer.addProp(metadataTable, "STR_VALUE", "TEXT");
			writer.addProp(metadataTable, "INT_VALUE", "INTEGER");
			writer.addProp(metadataTable, "NUM_VALUE", "NUMERIC(18,4)");
			writer.addProp(metadataTable, "BOOL_VALUE", "BOOLEAN");
			writer.addProp(metadataTable, "DATE_VAL", "DATE");
			writer.addProp(metadataTable, "TIMESTAMP_VAL", "TIMESTAMP");

			writer.addRelation(embeddingsTable, metadataTable, embeddingsTable+".SOURCE."+metadataTable+".SOURCE");
			
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
