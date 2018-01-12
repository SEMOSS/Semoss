package prerna.ds.datastax;

import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.evaluator.QueryStructExpressionIterator;
import prerna.test.TestUtilityMethods;

public class DataStaxTest {

	public static void main(String[] args) {
		QueryStruct2 qs = new QueryStruct2();
		qs.addSelector("book", null);
		qs.addSelector("book", "ISBN");
		qs.addSelector("recipe", null);
		qs.addSelector("recipe", "name");
		qs.addSelector("author", null);
		qs.addSelector("author", "gender"); //g.V().hasLabel("author")
		qs.addRelation("recipe", "book", "inner");
		qs.addRelation("author", "recipe", "inner");
		
		qs = new QueryStruct2();
		qs.addSelector("book", null); // g.V().hasLabel()
		qs.addSelector("book", "ISBN");
		qs.addSelector("recipe", null);
		qs.addSelector("ingredient", null);
		qs.addSelector("author", null);
		qs.addRelation("recipe", "book", "inner");
		qs.addRelation("recipe", "ingredient", "inner");
		qs.addRelation("author", "recipe", "inner");

//		qs = new QueryStruct2();
//		qs.addSelector("book", "name"); // g.V().hasLabel()
//		qs.addSelector("recipe", "name");
//		qs.addSelector("ingredient", "name");
//		qs.addRelation("recipe", "book", "inner");
//		qs.addRelation("recipe", "ingredient", "inner");
//		SimpleQueryFilter filter = new SimpleQueryFilter(
//				new NounMetadata(new QueryColumnSelector("book__name"), PixelDataType.COLUMN), 
//				"==", 
//				new NounMetadata("The Art of Simple Food: Notes, Lessons, and Recipes from a Delicious Revolution", PixelDataType.CONST_STRING)
//				);
//		qs.addFilter(filter);

		
//		qs = new QueryStruct2();
//		qs.addSelector("recipe", "name");
		
		
		
		qs = new QueryStruct2();
		qs.addSelector("recipe", null);
		qs.addSelector("book", null);
		qs.addRelation("recipe", "book", "inner");

		qs = new QueryStruct2();
		qs.addSelector("book", null);
		qs.addSelector("book", "year");
		
//		qs = new QueryStruct2();
//		qs.addSelector("book", "year");
		
		
		TestUtilityMethods.loadDIHelper();
//		// semoss integration
		String propFile = "C:\\Semoss\\datastax.smss";
		DataStaxGraphEngine engine = new DataStaxGraphEngine();
		engine.openDB(propFile);
		DataStaxInterpreter interpreter = (DataStaxInterpreter) engine.getQueryInterpreter2();
		interpreter.setQueryStruct(qs);
		IRawSelectWrapper wrapper = new QueryStructExpressionIterator(new DataStaxGraphIterator(interpreter.composeIterator(), qs), qs);
		while(wrapper.hasNext()) {
			System.out.println(wrapper.next());
		}
		
		
		// main simple test
//		DseCluster dseCluster = DseCluster.builder().addContactPoint("40.79.60.135").withPort(9042)
//				.withGraphOptions(new GraphOptions().setGraphName("test_connection")).build();
//		DseSession dseSession = dseCluster.connect();
//
//		GraphResultSet rs = dseSession.executeGraph("g.V().hasLabel(\"book\").values(\"id\")");
//		
//		// Iterating:
//		for (GraphNode n : rs) {
////			System.out.println(n);
//		}
//
		//META DATA
//		System.out.println("Connected");
//		Metadata metadata = dseCluster.getMetadata();
//		String schema = metadata.exportSchemaAsString();
//
//		KeyspaceMetadata keys = metadata.getKeyspace("test_connection");
//		Collection<TableMetadata> tables = keys.getTables();
//		for (TableMetadata table : tables) {
////			System.out.println(table.getName());
//		}
//		// System.out.println("Schema: \n" + schema);
//		GraphTraversalSource g = DseGraph.traversal(dseSession);
//
//		// Now you can use the Traversal source and use it **as if** it was
//		// working against a local graph, and with the usual TinkerPop API. All
//		// the communication with the DSE Graph server is done transparently.
//		List<Vertex> vertices = g.V().hasLabel("recipe").toList();
//		for (Vertex vert : vertices) {
//			// System.out.println(vert.toString());
//		}

		
		System.exit(0);
	}

}
