package prerna.engine.impl.json;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.NullNode;

import io.burt.jmespath.Expression;
import io.burt.jmespath.JmesPath;
import io.burt.jmespath.jackson.JacksonRuntime;
import prerna.engine.api.IDatabase;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.JsonInterpreter;
import prerna.util.Constants;
import prerna.util.Utility;

public class JsonAPIEngine2 extends JsonAPIEngine {
	
	// another json engine
	// that uses jmes path instead to aggregate results
	// evrything remains the same the way you get the output changes
	
	private static final Logger logger = LogManager.getLogger(JsonAPIEngine2.class);

	ObjectMapper mapper = null;
	JsonNode input = null;
	JmesPath<JsonNode> jmespath = new JacksonRuntime();

	public static final String ROOT = "root";
	public static final String COUNT = "COUNT";

	@Override
	protected void loadDocument()
	{
		try {
			getMapper();
			if(smssProp.containsKey("input_type") && ((String)smssProp.get("input_type")).equalsIgnoreCase("file"))
				input = mapper.readTree(new File(baseFolder + "/" + Utility.normalizePath(smssProp.getProperty("input_url"))));
		} catch (FileNotFoundException e) {
			logger.error(Constants.STACKTRACE, e);
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		}

	}

	private ObjectMapper getMapper()
	{
		if(this.mapper == null)
			mapper = new ObjectMapper();

		return mapper;
	}
	
	@Override
	protected Object getDocument(String json) {
		getMapper();
		JsonNode retNode = input; // if it is a document return it
		if (json != null) {
			try {
				retNode = mapper.readTree(json);
			} catch (Exception ex) {
				logger.error(Constants.STACKTRACE, ex);
			}
		}
		return retNode;
	}

	@Override
	protected Hashtable getOutput(Object doc, String [] jsonPaths, Hashtable retHash, String repeaterHeader, String repeaterValue)
	{
		// the selector is typically of the form
		// 			Expression<JsonNode> expression = jmespath.compile("foo[].[first, last]");
		// I will qualify this into 2 things
		// the root
		// and the selectors
		// the jsonpaths are broken into
		// the first element is the root
		// the other elements is what we need to select
		// the only thing to remember is repeater here
		// the prop should have the root

		
		// jackson will create this
		JsonNode data = null;
		
		
		String root = smssProp.getProperty(ROOT) + "[].";
		
		String selects = null;
		
		StringBuffer composer = new StringBuffer("[");

		String [] headers  = null;
		if(repeaterHeader != null) {
			headers = new String[jsonPaths.length + 1];
		} else {
			headers = new String[jsonPaths.length];
		}
		
		// leave the root out
		for(int pathIndex = 0; pathIndex < jsonPaths.length; pathIndex++) {
			// for multi
			// separate with comma
			if(pathIndex != 0) {
				composer.append(",");
			}
			
			// this is the case when we send a custom header
			// i.e. [custom_distance_name=distance]
			// we want to pull the data from distance but our header name
			// is custom_distance so we pass in those values accordingly
			// the opposite case is that these two are equal since
			// there is no = sign
			String jsonHeader = jsonPaths[pathIndex];
			String queryHeader = jsonHeader;
			// check aliasMap
			if(this.smssProp.get(jsonHeader) != null) {
				queryHeader = (String) this.smssProp.get(jsonHeader);
			}
			if(jsonHeader.contains("=")) {
				String[] split = jsonHeader.split("=");
				composer.append(split[1]);
				headers[pathIndex] = split[0];
			} else {
				// normal case
				composer.append(queryHeader);
				headers[pathIndex] = jsonHeader;
			}
		}
		
		composer.append("]");
		selects = composer.toString();

		// add the last header which is repeater
		if(repeaterHeader != null)
			headers[jsonPaths.length] = repeaterHeader;
		
		
		// the result is always array node of array node
		// need to find a way where I can fill this array node
		
		
		int numRows = 0;
		
		int totalRows = 0;

		ArrayNode input = null;
		if(retHash.containsKey("DATA"))
			input = (ArrayNode)	retHash.get("DATA");
		
		if(retHash.containsKey(COUNT))
			totalRows = (Integer) retHash.get(COUNT);

		// I can get everything I want in a single shot. Which is what I will do

		Expression<JsonNode> expression = jmespath.compile(root + selects);
		data = expression.search((JsonNode)doc);
		
		// the data is typically of the form ArrayNode of ArrayNode where each column is an array node within the bigger aray node
		if(!(data instanceof NullNode))
		{
			// I need to find the total length of it
			numRows = data.size();
			totalRows = totalRows + numRows;
			
			// need to make the repeater
			if(REPEATER != null)
			{
				ArrayNode repeaterNode = mapper.createArrayNode();
				for(int repeatIndex = 0;repeatIndex < numRows;repeatIndex++)
					repeaterNode.add(repeaterValue);
				((ArrayNode)data).add(repeaterNode);
			}
			
			if(input != null)
			{
				
				int colIndex = 0;
				
				// now also move all the input into this or other way whichever way
				// repeater node is also accounted for here
				for(colIndex = 0;colIndex < data.size();colIndex++)
				{
					ArrayNode colNode = (ArrayNode)data.get(colIndex);					
					((ArrayNode)input.get(colIndex)).addAll(colNode);
				}
			}
		}
				
		if(!retHash.containsKey("TYPES"))
			retHash.put("TYPES", getTypes(data));

		retHash.put("HEADERS", headers);
		if(input == null)
			retHash.put("DATA", data);
		else
			retHash.put("DATA", input);
			
		retHash.put("COUNT", totalRows);

		if(smssProp.containsKey("SEPARATOR"))
			retHash.put("SEPARATOR", smssProp.get("SEPARATOR"));

		logger.info("Output..  " + Utility.cleanLogString(data.toString()));

		return retHash;
	}
	
	@Override
	protected String [] getTypes(Object data2)
	{
		
		String [] types = new String[1];
		
		if( !(data2 instanceof NullNode))
		{
			
			ArrayNode mainData = (ArrayNode)data2;
			ArrayNode data = null;
			if(mainData.size() > 0 && mainData.get(0) instanceof ArrayNode)
			{
				data = (ArrayNode) mainData.get(0);
				
				types = new String[data.size()];
	
				for(int dataIndex = 0;dataIndex < data.size();dataIndex++)
				{
					
					JsonNode firstOne = data.get(dataIndex);
					JsonNodeType nt = firstOne.getNodeType();
					if(nt == JsonNodeType.NUMBER)
						//check if it double, 
						if(firstOne.isDouble()) {
							types[dataIndex] = "double";
						} else {
						types[dataIndex] = "int";
						}
					else
						types[dataIndex] = "String";
				}
			}
		}
		return types;
	}
	
	@Override
	public IQueryInterpreter getQueryInterpreter(){
		return new JsonInterpreter(this);
	}
	
	@Override
	public DATABASE_TYPE getDatabaseType() {
		return IDatabase.DATABASE_TYPE.JSON2;
	}

}
