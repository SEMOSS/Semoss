package prerna.util;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.burt.jmespath.Expression;
import io.burt.jmespath.JmesPath;
import io.burt.jmespath.jackson.JacksonRuntime;

public class BeanFiller {

	protected static final Logger classLogger = LogManager.getLogger(BeanFiller.class);

	// takes the data that is coming in from the json
	// gets a list of properties
	// and then fills it
	private static ObjectMapper mapper = new ObjectMapper();
	private static JmesPath<JsonNode> jmespath = new JacksonRuntime();
	
	private BeanFiller() {
		
	}
	
	// get the jsonNode for input
	public static JsonNode getJmesResult(String json, String jsonPattern) {
		try {
			Expression<JsonNode> expression = jmespath.compile(jsonPattern);
			JsonNode input = mapper.readTree(json);
			JsonNode result = expression.search(input);
			
			return result;
		} catch(Exception ex) {
			classLogger.error(Constants.STACKTRACE, ex);
		}
		return null;
	}

	// fills a single bean
	public static Object fillFromJson(String json, String jsonPattern, String [] beanProps, Object bean) {
		// make the class
		Object retObject = null;
		try {
			Expression<JsonNode> expression = jmespath.compile(jsonPattern);

			//AccessToken tok = mapper.readValue(json, AccessToken.class);
			JsonNode input = mapper.readTree(json);
			JsonNode result = expression.search(input);
			
			if((result instanceof ArrayNode) && result.get(0) instanceof ObjectNode) {
				// this is a multiple value
				List<Object> retList = new ArrayList<>();
				for(int resIndex = 0; resIndex < result.size(); resIndex++) {
					// I should possibly create a new instance everytime as well
					Object newBean = bean.getClass().newInstance();
					Object newObject = null;
					newObject = fillSingleObjectFromMap(result.get(resIndex), beanProps, newBean);
					//else
					//	System.out.println("Need to find if this is a Map.. ");
					retList.add(newObject);
				}
				retObject = retList;
			} else {
				retObject = fillSingleObject(result, beanProps, bean);
			}
		} catch(Exception ex) {
			classLogger.error(Constants.STACKTRACE, ex);
		}
		return retObject;
	}
	
	/**
	 * 
	 * @param node
	 * @return
	 */
	public static boolean isJsonArray(JsonNode node) {
		boolean array = false;
		// get the first element
		// if it is an array then proceed with that.. 
		if(node.size() > 0) {
			JsonNode firstNode = node.get(0);
			if(firstNode instanceof ArrayNode) {
				array = true;
			}
		}
		return array;
	}
	
	/**
	 * 
	 * @param result
	 * @param beanProps
	 * @param bean
	 * @return
	 */
	public static Object fillSingleObject(JsonNode result, String [] beanProps, Object bean) {
		try {
			for(int inputIndex = 0;result != null && inputIndex < result.size();inputIndex++) {
				String thisInput = result.get(inputIndex).asText();
				if(beanProps.length > inputIndex) {
					String beanProp = beanProps[inputIndex];
					if(beanProp.startsWith("add_")) {
						beanProp = beanProp.replaceAll("add_", "");
						List thisList = null;
						Object listObj = BeanUtils.getProperty(bean, beanProp);
						if(listObj != null) {
							thisList = (List)listObj;
						} else {
							thisList = new ArrayList();
						}
						thisList.add(thisInput);
						BeanUtils.setProperty(bean, beanProp, listObj);
					} else {
						BeanUtils.setProperty(bean, beanProp, thisInput);
					}
				}	
				// add to the other data
				else {
					BeanUtils.setProperty(bean, "extra", thisInput);
				}
			}
		} catch (IllegalAccessException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (InvocationTargetException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (NoSuchMethodException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} 
		
		return bean;
		
	}

	/**
	 * Fill an object based on a map input
	 * @param result
	 * @param beanProps
	 * @param bean
	 * @return
	 */
	public static Object fillSingleObjectFromMap(JsonNode result, String [] beanProps, Object bean) {
		try {
			for(int inputIndex = 0;result != null && inputIndex < beanProps.length;inputIndex++) {
				// grab the bean
				String beanProp = beanProps[inputIndex];
				
				JsonNode thisInputObj = result.get(beanProp);
				if(thisInputObj.isArray()) {
					//TODO: i should really be doign this as an array
					//TODO: i should really be doign this as an array

					StringBuilder concat = new StringBuilder();
					int innerArraySize = thisInputObj.size();
					concat.append( thisInputObj.get(0).asText() );
					for(int innerArrayIndex = 1; innerArrayIndex < innerArraySize; innerArrayIndex++) {
						concat.append(", ").append(thisInputObj.get(innerArrayIndex));
					}
					// this is adding as a string
					BeanUtils.setProperty(bean, beanProp, concat.toString());
				} else {
					// grab as string
					String thisInput = thisInputObj.asText();
					if(result.size() > inputIndex) {
						// WHEN DO I WANT THIS??? WHY CAN'T I RETURN AN ARRAY VIA THE PATH
						if(beanProp.startsWith("add_")) {
							beanProp = beanProp.replaceAll("add_", "");
							List<Object> thisList = null;
							Object listObj = BeanUtils.getProperty(bean, beanProp);
							if(listObj != null) {
								thisList = (List<Object>) listObj;
							} else {
								thisList = new ArrayList<Object>();
							} 
							thisList.add(thisInput);
							BeanUtils.setProperty(bean, beanProp, listObj);
						} 
						// normal, just add it
						else {
							BeanUtils.setProperty(bean, beanProp, thisInput);
						}
					}	
					// add to the other data
					else {
						BeanUtils.setProperty(bean, "extra", thisInput);
					}
				}
			}
		} catch (IllegalAccessException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (InvocationTargetException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (NoSuchMethodException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} 
		
		return bean;
	}

	
	public static String getJson(Object object) throws Exception {
		return mapper.writeValueAsString(object);
	}
	
}
