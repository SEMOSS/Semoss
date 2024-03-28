package prerna.tcp.client.workers;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.IStringExportProcessor;
import prerna.om.Insight;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.tcp.PayloadStruct;
import prerna.util.Utility;

public class NativePyEngineWorker implements Runnable {
	
	// responsible for doing all of the work from an engine's perspective
	// the server sends information to semoss core to execute something
	// this thread will work through in terms of executing it
	// and then send the response back
	PayloadStruct ps = null;
	public static final int MAX_ROWS = 50;
	PayloadStruct output = null;
	User user = null;
	Insight insight = null;
	
	public NativePyEngineWorker(User user, PayloadStruct ps)
	{
		this.ps = ps;
		this.user = user;
	}

	public NativePyEngineWorker(User user, PayloadStruct ps, Insight insight)
	{
		this.ps = ps;
		this.user = user;
		this.insight = insight;
	}

	@Override
	public void run() 
	{
		
		try
		{
			// TODO Auto-generated method stub
			String engineId = ps.objId;
			
			// TODO: *****************need to do a security check *************
			boolean canAccess = SecurityEngineUtils.userCanViewEngine(user, engineId); 
			
			if(canAccess)
			{
				IEngine engine = null;
				if(ps.engineType.equalsIgnoreCase("DATABASE") && ps.methodName.equalsIgnoreCase("execquery"))
				{
					engine = Utility.getDatabase(engineId);
					{
						IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper((IDatabaseEngine)engine, ps.payload[0] + "");
						wrapper.execute();
						{
							// do the logic of converting it into 
							String fileLocation = null;
							if(this.insight == null || (ps.payload.length > 1 && ps.payload[1].equals("json"))) {
								JSONArray jsonPayload = Utility.writeResultToJsonObject(wrapper, null, new IStringExportProcessor() {
									// we need to replace all inner quotes with ""
									@Override
									public String processString(String input) {
										return input.replace("\"", "\\\"");
									}
								});
								ps.payload = new Object[] {jsonPayload};
							}
							else
							{
								fileLocation = insight.getInsightFolder() + "/a_" + Utility.getRandomString(5) + ".json";
								Utility.writeResultToJson(fileLocation, wrapper, null, new IStringExportProcessor() {
									// we need to replace all inner quotes with ""
									@Override
									public String processString(String input) {
										return input.replace("\"", "\\\"");
									}
								});
								fileLocation = fileLocation.replace("\\","/");
								ps.payload = new Object[] {fileLocation};
							}
						}
					}
				}
				else
				{
					if(ps.engineType.equalsIgnoreCase("MODEL"))
						engine = Utility.getModel(engineId);
					else if(ps.engineType.equalsIgnoreCase("STORAGE"))
						engine = Utility.getStorage(engineId);
					else if(ps.engineType.equalsIgnoreCase("DATABASE"))
						engine = Utility.getDatabase(engineId);
					else if(ps.engineType.equalsIgnoreCase("VECTOR"))
						engine = Utility.getVectorDatabase(engineId);
					else if(ps.engineType.equalsIgnoreCase("FUNCTION"))
						engine = Utility.getFunctionEngine(engineId);
					
					Method method = findEngineMethod(engine, ps.methodName, ps.payloadClasses);
					Object retObject = method.invoke(engine, ps.payload);
		
					// the map that comes may not be fully serializable
					if(retObject instanceof Map)
					{
						if(((Map) retObject).containsKey(RDBMSNativeEngine.RESULTSET_OBJECT))
						{
							JSONArray retArray = convertToJSONArray((ResultSet)((Map) retObject).get(RDBMSNativeEngine.RESULTSET_OBJECT));
							
							//ps.payload = new Object [] {"helo"};
						}
						else
						{
							Map <String, Object> outputMap = normalizeMap((Map <String, Object>)retObject);
							ps.payload = new Object[] {outputMap};
						}
					}
					else
					{
						// need to check for serialization
						ps.payload = new Object[] {retObject};
					}
				}					
			}
			else
			{
				// this should just go into the catch below
				throw new IllegalArgumentException("Engine " + engineId + " does not exist or user does not have access to it");
			}
			// got the response
			ps.response = true;
			
		} catch(Exception ex)
		{
			ex.printStackTrace();
			
		    // Get the message from the current exception
		    String errorMessage = (ex.getCause() != null) ? ex.getCause().getMessage() : ex.getLocalizedMessage();

		    // if its null, pass a generic message
		    if (errorMessage == null) {
		        errorMessage = "Runtime Error Processing Python Command";
		    }
		    
			ps.ex = errorMessage;
			ps.response = true;
		}
		output = ps;
	}

    public Method findEngineMethod(IEngine engine, String methodName, Class<?> [] arguments) throws NoSuchMethodException {
    	Method retMethod = null;
        Class<?> currentClass = engine.getClass();

        while (currentClass != null) {
            try {
                // Try to find the method in the current class
                if (arguments != null) {
                    retMethod = currentClass.getDeclaredMethod(methodName, arguments);
                } else {
                    retMethod = currentClass.getDeclaredMethod(methodName);
                }
                if (retMethod != null) {
                    // If the method is found, break out of the loop
                    break;
                }
            } catch (NoSuchMethodException e) {
                // If the method is not found in the current class, continue with the superclass
            } catch (SecurityException e) {
                e.printStackTrace();
            }
            // Move to the superclass for the next iteration
            currentClass = currentClass.getSuperclass();
        }
        
        if (retMethod == null) {
        	throw new NoSuchMethodException("Unable to find method called " + methodName + " for engine type " + engine.getCatalogType().toString());
        }

        return retMethod;
    }
    
    private Map <String, Object> normalizeMap(Map <String, Object> input)
    {
    	// parse through the objects
    	// if the object is not serializable no go
    	// if the object is a result set
    	// turn it into CachedRowsetImpl
    	Map <String, Object> output = new HashMap<String, Object>();
    	
    	Iterator <String> keys = input.keySet().iterator();
    	
    	while(keys.hasNext())
    	{
    		String key = keys.next();
    		Object obj = input.get(key);
    		
    		if(obj instanceof ResultSet)
    		{
    			JSONArray jsonArray;
				try {
					jsonArray = convertToJSONArray((ResultSet)obj);
					output.put(key, jsonArray);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}
    		else if(obj instanceof Serializable)
    		{
    			output.put(key, obj);
    		}
    	}
    	return output;
    }
    
    public PayloadStruct getOutput()
    {
    	return this.output;
    }

    
    public JSONArray convertToJSONArray(ResultSet resultSet)
            throws Exception {
        JSONArray jsonArray = new JSONArray();
        while (resultSet.next()) {
            JSONObject obj = new JSONObject();
            int total_rows = resultSet.getMetaData().getColumnCount();
            for (int i = 0; i < total_rows; i++) {
                obj.put(resultSet.getMetaData().getColumnLabel(i + 1)
                        .toLowerCase(), resultSet.getObject(i + 1));

            }
            jsonArray.put(obj);
        }
        return jsonArray;// .toString();
    }
	
}
