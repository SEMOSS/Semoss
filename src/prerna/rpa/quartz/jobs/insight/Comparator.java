package prerna.rpa.quartz.jobs.insight;

import java.util.HashMap;
import java.util.Map;

public enum Comparator {
	EQUALS("=="),
	NOT_EQUALS("!="),
	GREATER_THAN(">"),
	LESS_THAN("<");
	
	private final String symbol;
	
	private static Map<String, Comparator> symbolToComparator = new HashMap<>();
	
	// Map the symbol to the Comparator
	static {
		for (Comparator comparator : Comparator.values()) {
			symbolToComparator.put(comparator.getSymbol(), comparator);
		}
	}
	
	Comparator(final String symbol) {
		this.symbol = symbol;
	}

	public static Comparator getComparatorFromSymbol(String symbol) {
		return symbolToComparator.get(symbol);
	}
	
	public String getSymbol() {
		return symbol;
	}
	
}
