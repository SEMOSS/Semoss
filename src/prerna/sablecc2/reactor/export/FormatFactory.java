package prerna.sablecc2.reactor.export;

public class FormatFactory {

	public static IFormatter getFormatter(String formatType) {
		
		switch(formatType.toUpperCase()) {
		
		case "TABLE": {
			return new TableFormatter();
		}
		
		case "GRID": {
			return new TableFormatter();
		}
		
		case "GRAPH": {
			return new GraphFormatter();
		}
		
		case "JSON": {
			return new JsonFormatter();
		}
		
		case "KEYVALUE": {
			return new KeyValueFormatter();
		}
		
		case "CLUSTERGRAMMAR": {
			return new ClustergramFormatter();
		}
		
		case "CLUSTERGRAM" : {
			return new ClustergramFormatter();
		}
		
		case "HIERARCHY" : {
			return new HierarchyFormatter();
		}
		
		default : {
			return new TableFormatter();
		}
		
		}
	}
}
