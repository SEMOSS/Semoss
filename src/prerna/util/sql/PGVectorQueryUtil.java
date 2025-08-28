/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.util.sql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
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

  @Override
  public void enhanceConnection(Connection con) {
    try (Statement stmt = con.createStatement()) {
      stmt.execute(addVectorExtension());
    } catch (SQLException e) {
      classLogger.warn("Unable to create the vector extension");
      classLogger.error(Constants.STACKTRACE, e);
    }
  }

  public String addVectorExtension() {
    return "CREATE EXTENSION IF NOT EXISTS vector;";
  }

  public String createEmbeddingsTable(String table) {
    return "CREATE TABLE IF NOT EXISTS "
        + table
        + "("
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
    return "CREATE TABLE IF NOT EXISTS "
        + table
        + "("
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

  public void createOWL(
      PGVectorDatabaseEngine engine, String embeddingsTable, String metadataTable) {
    try (WriteOWLEngine writer = engine.getOWLEngineFactory().getWriteOWL()) {
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

      writer.addRelation(
          embeddingsTable,
          metadataTable,
          embeddingsTable
              + "."
              + VectorDatabaseCSVTable.SOURCE
              + "."
              + metadataTable
              + "."
              + VectorDatabaseMetadataCSVTable.SOURCE);

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
