package prerna.ds.py;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.gson.Gson;

import java.io.IOException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class PandasTimestampDeserializer extends SimpleModule {
	private static final long serialVersionUID = -2389464189802719725L;

	public static final ObjectMapper MAPPER = new ObjectMapper()
			.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
			.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
			.registerModule(new PandasTimestampDeserializer());
    
    public PandasTimestampDeserializer() {
        addDeserializer(Object.class, new StdDeserializer<Object>(Object.class) {
			private static final long serialVersionUID = -4179939507094287588L;

			@Override
			public Object deserialize(final JsonParser jp, final DeserializationContext ctxt)
					throws IOException, JsonProcessingException {
				String payloadString = jp.getText();
				
		        String jsonString = removeTimestamp(payloadString);
		        
		        Object obj = new Gson().fromJson(jsonString, Object.class);
		        
		        return obj;
		        
		        
		        // leaving this logic for now in case we come back to it
//		        ObjectMapper objectMapper = new ObjectMapper();
//		     // Convert the JSON string to a JSON object
//		        JsonNode jsonNode = objectMapper.readTree(jsonString);
//		        
//		        if (jsonNode.isObject()) {
//		            // Convert JsonObject to Map
//		            Map<String, Object> map = objectMapper.convertValue(jsonNode, Map.class);
//		            return map;
//		        } else if (jsonNode.isArray()) {
//		        	 // Convert JsonNode to List
//		            List<Object> list = objectMapper.convertValue(jsonNode, List.class);
//		            return list;
//		        }
//		        
//		        System.out.println("It's neither a JsonObject nor a JsonArray.");
//		        return null;
			}
		});
    }


	/**
	 * Change a String from having Timestamp(<timestamp value>) to just <timestamp value>
	 * @param jsonText				Python Dictionary that we were unable to desearlize and comes back as a String
	 * @return						String where we tried to remove pandas timestamps
	 */
    public static String removeTimestamp(String jsonText) {
        // String patternString = "(, |\\[|\\{)(Timestamp\\('([^']+)'\\))(\\]|\\}|,)";
        String patternString = "(, |\\[|\\{)(Timestamp\\('([^']+)'\\))(?=(,|\\]|\\}|,))";
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(jsonText);
        
        StringBuffer modifiedJson = new StringBuffer();
        while (matcher.find()) {
            String beforeDelimiter = matcher.group(1);
            String timestampReplacement = matcher.group(3);
            //String afterDelimiter = matcher.group(4);
            String replacement = beforeDelimiter + "\'" + timestampReplacement + "\'";
            matcher.appendReplacement(modifiedJson, replacement);
        }
        matcher.appendTail(modifiedJson);

        return modifiedJson.toString();
    }
}
