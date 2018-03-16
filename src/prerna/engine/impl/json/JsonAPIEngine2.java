package prerna.engine.impl.json;

import io.burt.jmespath.Expression;
import io.burt.jmespath.JmesPath;
import io.burt.jmespath.jackson.JacksonRuntime;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.impl.AbstractEngine;
import prerna.query.interpreters.IQueryInterpreter2;
import prerna.query.interpreters.JsonInterpreter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.NullNode;

public class JsonAPIEngine2 extends JsonAPIEngine {
	
	// another json engine
	// that uses jmes path instead to aggregate results
	// evrything remains the same the way you get the output changes
	
	private static final Logger logger = LogManager.getLogger(AbstractEngine.class.getName());
	
	ObjectMapper mapper = null;
	JsonNode input = null;
	JmesPath<JsonNode> jmespath = new JacksonRuntime();
	
	public static final String root = "root";



	protected void loadDocument()
	{
		try {
			getMapper();
			if(prop.containsKey("input_type") && ((String)prop.get("input_type")).equalsIgnoreCase("file"))
				input = mapper.readTree(new File(baseFolder + "/" + prop.getProperty("input_url")));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
		
	
	private ObjectMapper getMapper()
	{
		if(this.mapper == null)
			mapper = new ObjectMapper();

		return mapper;
	}
	
	@Override
	protected Object getDocument(String json)
	{
		getMapper();
		JsonNode retNode = input ; // if it is a document return it
		if(json != null)
		{
			try
			{
				retNode = mapper.readTree(json);
			}catch(Exception ex)
			{
				
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
		
		
		String root = prop.getProperty(this.root) + "[].";
		
		String selects = null;
		
		StringBuffer composer = new StringBuffer("[");

		String [] headers  = null;
		
		if(repeaterHeader != null)
			headers = new String[jsonPaths.length + 1];
		else
			headers = new String[jsonPaths.length];
		
		// leave the root out
		for(int pathIndex = 0;pathIndex < jsonPaths.length;pathIndex++)
		{
			if(pathIndex == 0)
				composer.append(jsonPaths[pathIndex]);
			else
				composer.append(",").append(jsonPaths[pathIndex]);
			headers[pathIndex] = jsonPaths[pathIndex];
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
		
		if(retHash.containsKey("COUNT"))
			totalRows = (Integer)	retHash.get("COUNT");

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
			if(repeater != null)
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

		System.out.println("Output..  " + data);
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
					// there is no double in json type so... 
					if(nt == JsonNodeType.NUMBER)
						types[dataIndex] = "int";
					else
						types[dataIndex] = "String";
				}
			}
		}
		return types;
	}
	
	@Override
	public IQueryInterpreter2 getQueryInterpreter2(){
		return new JsonInterpreter(this);
	}
	
	@Override
	public ENGINE_TYPE getEngineType() {
		// TODO Auto-generated method stub
		return prerna.engine.api.IEngine.ENGINE_TYPE.JSON2;
	}


}
