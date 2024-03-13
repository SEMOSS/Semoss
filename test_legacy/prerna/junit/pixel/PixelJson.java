package prerna.junit.pixel;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import prerna.util.gson.GsonUtility;

public class PixelJson {
	
	private final JsonObject json;
	private final String pixelExpression;
	private final String pixelOutput;
	
	private static final Gson GSON_PRETTY = GsonUtility.getDefaultGson(true);
	
	private static final String LS = System.getProperty("line.separator");

	public PixelJson(JsonObject json) {
		this.json = json;
		
		if (!(json.has("pixelExpression") && json.has("output"))) {
			String message = LS
					+ "Each expected JSON for this test must contain both a \"pixelExpression\" and its corresponding \"output\" in the form: " + LS
					+ LS
					+ "{" + LS
					+ "   \"pixelExpression\":\"<pixel>;\"," + LS
					+ "   \"output\":{" + LS
					+ "      <output json>"+ LS
					+ "   }(,... <any additional members, which are ignored>)" + LS
					+ "}" + LS
					+ LS
					+ "Or (when compare_all = true): " + LS
					+ LS
					+ "[" + LS
					+ "   {" + LS
					+ "      \"pixelExpression\":\"<pixel>;\"," + LS
					+ "      \"output\":{" + LS
					+ "         <output json>"+ LS
					+ "      }(,... <any additional members, which are ignored>)" + LS
					+ "   }(,... <additional pixel outputs from the recipe>)" + LS
					+ "]";
			
			throw new IllegalArgumentException(message);
		}
		
		this.pixelExpression = json.get("pixelExpression").getAsString();
		this.pixelOutput = GSON_PRETTY.toJson(json.get("output"));
	}

	public JsonObject getJson() {
		return json;
	}

	public String getPixelOutput() {
		return pixelOutput;
	}

	public String getPixelExpression() {
		return pixelExpression;
	}
	
}
