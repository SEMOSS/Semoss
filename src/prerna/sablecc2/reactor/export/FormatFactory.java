package prerna.sablecc2.reactor.export;

public class FormatFactory {

	public static Formatter getFormatter(String formatType) {
		
		switch(formatType.toUpperCase()) {
		
		case "TABLE": {
			return new TableFormatter();
		}
		
		case "GRAPH": {
			return new GraphFormatter();
		}
		
		case "JSON": {
			return new JsonFormatter();
		}
		
		default : {
			return new TableFormatter();
		}
		}
		
	}
}
