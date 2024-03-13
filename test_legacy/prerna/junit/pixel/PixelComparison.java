package prerna.junit.pixel;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class PixelComparison {

	private final PixelJson expectedPixelJson;
	private final PixelJson actualPixelJson;
	private final Map<String, Object> differences;
	private final boolean different;
	
	public PixelComparison(PixelJson expectedPixelJson, PixelJson actualPixelJson, List<String> excludePaths, boolean ignoreOrder, PixelUnit runner) throws IOException {
		this.expectedPixelJson = expectedPixelJson;
		this.actualPixelJson = actualPixelJson;
		if (!expectedPixelJson.getPixelExpression().equals(actualPixelJson.getPixelExpression())) {
			throw new IllegalArgumentException("Unable to compare the results; the expected and actual pixel expressions are different.");
		}
		
		differences = runner.deepDiff(expectedPixelJson.getPixelOutput(), actualPixelJson.getPixelOutput(), excludePaths, ignoreOrder);
		different = differences.size() > 0;
	}
	
	public String getPixelExpression() {
		
		// Doesn't matter which one we pull from, since they are guaranteed to be the same
		return actualPixelJson.getPixelExpression();
	}
	
	public String getExpectedPixelOutput() {
		return expectedPixelJson.getPixelOutput();
	}
	
	public String getActualPixelOutput() {
		return actualPixelJson.getPixelOutput();
	}
	
	public Map<String, Object> getDifferences() {
		return differences;
	}
	
	public boolean isDifferent() {
		return different;
	}
	
	public boolean isDifferent(boolean ignoreAddedDictionary, boolean ignoreAddedIterable) {
		Map<String, Object> differences = this.differences;
		if (ignoreAddedDictionary) {
			differences.remove("dictionary_item_added");
		}
		if (ignoreAddedIterable) {
			differences.remove("iterable_item_added");
		}
		return differences.size() > 0;
	}
	
}
