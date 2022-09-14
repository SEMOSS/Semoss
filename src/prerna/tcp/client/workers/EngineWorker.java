package prerna.tcp.client.workers;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.sun.rowset.CachedRowSetImpl;

import prerna.engine.api.IEngine;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.SocketClient;
import prerna.util.Utility;

public class EngineWorker implements Runnable {
	
	// responsible for doing all of the work from an engine's perspective
	// the server sends information to semoss core to execute something
	// this thread will work through in terms of executing it
	// and then send the response back
	SocketClient sc = null;
	PayloadStruct ps = null;
	public static final int MAX_ROWS = 50;
	
	public EngineWorker(SocketClient sc, PayloadStruct ps)
	{
		this.sc = sc;
		this.ps = ps;
	}

	@Override
	public void run() 
	{
		
		try
		{
			// TODO Auto-generated method stub
			String engineId = ps.objId;
			// TODO: *****************need to do a security check *************
			IEngine engine = Utility.getEngine(engineId);
			Method method = findEngineMethod(engine, ps.methodName, ps.payloadClasses);
			Object retObject = method.invoke(engine, ps.payload);

			// the map that comes may not be fully serializable
			if(retObject instanceof Map)
			{
				Map <String, Object> outputMap = normalizeMap((Map <String, Object>)retObject);
				ps.payload = new Object[] {outputMap};
			}
			else
			{
				// need to check for serialization
				ps.payload = new Object[] {retObject};
			}
			
			// got the response
			ps.response = true;
			
		}catch(Exception ex)
		{
			ex.printStackTrace();
			ps.ex = ex.getLocalizedMessage();
			ps.response = true;
		}
		sc.executeCommand(ps);
	}

    public Method findEngineMethod(IEngine engine, String methodName, Class [] arguments)
    {
    	Method retMethod = null;
    	
    	// look for it in the child class if not parent class
    	// we can even cache this later
    	try {
			if(arguments != null)
			{
				try
				{
					retMethod = engine.getClass().getDeclaredMethod(methodName, arguments);
				}catch(Exception ex)
				{
					
				}
				if(retMethod == null)
					retMethod = engine.getClass().getSuperclass().getDeclaredMethod(methodName, arguments);
				
			}
			else
			{
				try
				{
					retMethod = engine.getClass().getDeclaredMethod(methodName);				
				}catch(Exception ex)
				{
					
				}
				if(retMethod == null)
					retMethod = engine.getClass().getSuperclass().getDeclaredMethod(methodName, arguments);
			}
			//LOGGER.info("Found the method " + retMethod);
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
    			try {
					// move this CacheRowSetImpl
					CachedRowSetImpl impl = new CachedRowSetImpl();
					impl.setMaxRows(MAX_ROWS);
					impl.populate((ResultSet)obj);
					output.put(key, impl);
				} catch (SQLException e) {
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

	
}
