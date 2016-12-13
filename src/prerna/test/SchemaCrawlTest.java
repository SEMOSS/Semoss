package prerna.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

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

	public static void main(String[] args) throws Exception {
		String url = "jdbc:mysql://HOST:PORT/SCHEMA?useSSL=false";
		String user = "username";
		String password = "password";

		Connection conn = DriverManager.getConnection(url, user, password);
		if(conn != null) {
			System.out.println("Connection successful.");
		}
		Statement stmt = conn.createStatement();

		SchemaCrawlerOptions options = new SchemaCrawlerOptions();
		// Set what details are required in the schema - this affects the time taken to crawl the schema
		options.setSchemaInfoLevel(SchemaInfoLevelBuilder.standard());
		options.setRoutineInclusionRule(new ExcludeAll());
		ArrayList<String> tableTypes = new ArrayList<String>();
		tableTypes.add("table");
		options.setTableTypes(tableTypes);
		options.setSchemaInclusionRule(new RegularExpressionInclusionRule("SCHEMA"));

		Catalog catalog = SchemaCrawlerUtility.getCatalog(conn, options);

		for (Schema schema: catalog.getSchemas())
		{
			System.out.println(schema);
			for (Table table: catalog.getTables(schema))
			{
				System.out.println("Table: " + table);
				System.out.println("Number of cols: " + table.getColumns().size());
				for (Column column: table.getColumns())
				{
					System.out.print("\tCol: " + column.getName() + "\tType: " + column.getType());
					if(column.isPartOfPrimaryKey()) {
						System.out.print("\t<- primary key");
					}
					System.out.println();
				}
				
				for(ForeignKey fk : table.getForeignKeys()) {
					List<ForeignKeyColumnReference> fkcrList = fk.getColumnReferences();
					System.out.println("Foreign Key Name: " + fk.getName());
					for(ForeignKeyColumnReference fkcr : fkcrList) {
						System.out.println("From " + fkcr.getPrimaryKeyColumn() + " to " + fkcr.getForeignKeyColumn());
					}
				}
			}
		}
	}

}
