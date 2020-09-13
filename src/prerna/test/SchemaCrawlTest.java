package prerna.test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Constants;
import prerna.util.DIHelper;
import schemacrawler.schema.Catalog;
import schemacrawler.schema.Column;
import schemacrawler.schema.ForeignKey;
import schemacrawler.schema.ForeignKeyColumnReference;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.ExcludeAll;
import schemacrawler.schemacrawler.RegularExpressionInclusionRule;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaInfoLevelBuilder;
import schemacrawler.utility.SchemaCrawlerUtility;

public class SchemaCrawlTest {
	private static final Logger logger = LogManager.getLogger(SchemaCrawlTest.class.getName());
	private static final String SQL_URL = "sql_url";
	private static final String SQL_USERNAME = "sql_user";
	private static final String SQL_PASSWORD = "sql_password";
	private static final String CONFIGURATION_FILE = "config.properties";
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public static void main(String[] args) throws Exception {
		Connection conn = null;
		try(InputStream input = new FileInputStream(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + CONFIGURATION_FILE)) {
			Properties prop = new Properties();

			prop.load(input);

			String url = prop.getProperty(SQL_URL);
			String user = prop.getProperty(SQL_USERNAME);
			String password = prop.getProperty(SQL_PASSWORD);

			conn = DriverManager.getConnection(url, user, password);
			if(conn != null) {
				logger.debug("Connection successful.");
			}

			SchemaCrawlerOptions options = new SchemaCrawlerOptions();
			// Set what details are required in the schema - this affects the time taken to crawl the schema
			options.setSchemaInfoLevel(SchemaInfoLevelBuilder.standard());
			options.setRoutineInclusionRule(new ExcludeAll());
			ArrayList<String> tableTypes = new ArrayList<>();
			tableTypes.add("table");
			options.setTableTypes(tableTypes);
			options.setSchemaInclusionRule(new RegularExpressionInclusionRule("SCHEMA"));

			Catalog catalog = SchemaCrawlerUtility.getCatalog(conn, options);

			for (Schema schema: catalog.getSchemas()) {
				logger.debug(schema);
				for (Table table: catalog.getTables(schema)) {
					logger.debug("Table: " + table);
					logger.debug("Number of cols: " + table.getColumns().size());
					for (Column column: table.getColumns()) {
						logger.debug("\tCol: " + column.getName() + "\tType: " + column.getType());
						if(column.isPartOfPrimaryKey()) {
							logger.debug("\t<- primary key");
						}
					}

					for(ForeignKey fk : table.getForeignKeys()) {
						List<ForeignKeyColumnReference> fkcrList = fk.getColumnReferences();
						logger.debug("Foreign Key Name: " + fk.getName());
						for(ForeignKeyColumnReference fkcr : fkcrList) {
							logger.debug("From " + fkcr.getPrimaryKeyColumn() + " to " + fkcr.getForeignKeyColumn());
						}
					}
				}
			}
		} catch (IOException ex) {
			logger.error("Error with loading properties in config file" + ex.getMessage());
		} finally {
			conn.close();
		}
	}
}
