package prerna.util;

import io.burt.jmespath.Expression;
import io.burt.jmespath.JmesPath;
import io.burt.jmespath.jackson.JacksonRuntime;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.commons.beanutils.BeanUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class BeanFiller {

	// takes the data that is coming in from the json
	// gets a list of properties
	// and then fills it
	static ObjectMapper mapper = new ObjectMapper();
	static JmesPath<JsonNode> jmespath = new JacksonRuntime();
	
	// get the jsonNode for input
	public static JsonNode getJmesResult(String json, String jsonPattern)
	{
		try {
			
			Expression<JsonNode> expression = jmespath.compile(jsonPattern);

			//AccessToken tok = mapper.readValue(json, AccessToken.class);
			JsonNode input = mapper.readTree(json);
			JsonNode result = expression.search(input);
			
			return result;
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
		return null;
	}

	// fills a single bean
	public static Object fillFromJson(String json, String jsonPattern, String [] beanProps, Object bean)
	{
		// make the class
		
		Object retObject = null;
		
		try {
			
			Expression<JsonNode> expression = jmespath.compile(jsonPattern);

			//AccessToken tok = mapper.readValue(json, AccessToken.class);
			JsonNode input = mapper.readTree(json);
			JsonNode result = expression.search(input);
			
			System.out.println("Is array  " + (result instanceof ArrayNode));
			if((result instanceof ArrayNode) && result.get(0) instanceof ObjectNode) // this is a multiple value
			{
				List <Object> retList = new Vector();

				
				for(int resIndex = 0;resIndex < result.size();resIndex++)
				{
					// I should possibly create a new instance everytime as well
					Object newBean = bean.getClass().newInstance();
					Object newObject = null;
					newObject = fillSingleObjectFromMap(result.get(resIndex), beanProps, newBean);
					//else
					//	System.out.println("Need to find if this is a Map.. ");
					retList.add(newObject);
				}
				retObject = retList;
			}
			else
				retObject = fillSingleObject(result, beanProps, bean);
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
		return retObject;
	}
	
	public static boolean isJsonArray(JsonNode node)
	{
		boolean array = false;
		// get the first element
		// if it is an array then proceed with that.. 
		if(node.size() > 0)
		{
			JsonNode firstNode = node.get(0);
			if(firstNode instanceof ArrayNode)
				array = true;
		}
		return array;
	}
	
	
	public static Object fillSingleObject(JsonNode result, String [] beanProps, Object bean)
	{
		try
		{
			for(int inputIndex = 0;result != null && inputIndex < result.size();inputIndex++)
			{
				String thisInput = result.get(inputIndex).asText();
				if(beanProps.length > inputIndex)
				{
					String beanProp = beanProps[inputIndex];
					if(beanProp.startsWith("add_"))
					{
						beanProp = beanProp.replaceAll("add_", "");
						List thisList = null;
						Object listObj = BeanUtils.getProperty(bean, beanProp);
						if(listObj != null)
							thisList = (List)listObj;
						else
							thisList = new ArrayList();
						thisList.add(thisInput);
						
						BeanUtils.setProperty(bean, beanProp, listObj);
					}
					else
					{
						BeanUtils.setProperty(bean, beanProp, thisInput);
					}
				}	
				// add to the other data
				else
				{
					BeanUtils.setProperty(bean, "extra", thisInput);
				}
				
			}
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		return bean;
		
	}

	public static Object fillSingleObjectFromMap(JsonNode result, String [] beanProps, Object bean)
	{
		try
		{
			for(int inputIndex = 0;result != null && inputIndex < beanProps.length;inputIndex++)
			{
				String thisInput = result.get(beanProps[inputIndex]).asText();
				if(result.size() > inputIndex)
				{
					String beanProp = beanProps[inputIndex];
					if(beanProp.startsWith("add_"))
					{
						beanProp = beanProp.replaceAll("add_", "");
						List thisList = null;
						Object listObj = BeanUtils.getProperty(bean, beanProp);
						if(listObj != null)
							thisList = (List)listObj;
						else
							thisList = new ArrayList();
						thisList.add(thisInput);
						
						BeanUtils.setProperty(bean, beanProp, listObj);
					}
					else
					{
						BeanUtils.setProperty(bean, beanProp, thisInput);
					}
				}	
				// add to the other data
				else
				{
					BeanUtils.setProperty(bean, "extra", thisInput);
				}
				
			}
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		return bean;
		
	}

	
	
	public static String getJson(Object object) throws Exception
	{
		return mapper.writeValueAsString(object);
	}
	
}
