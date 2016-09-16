package prerna.sablecc;

import java.sql.Connection;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.H2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.util.Console;


public abstract class BaseJavaReactor extends AbstractReactor{

	ITableDataFrame dataframe = null;
	PKQLRunner pkql = new PKQLRunner();
	boolean frameChanged = false;
	SecurityManager curManager = null;
	SecurityManager reactorManager = null;
	
	public Console System = new Console();
	
	
	public BaseJavaReactor()
	{
		// empty constructor creates a frame
		dataframe = new H2Frame();
	}
	
	public void setCurSecurityManager(SecurityManager curManager)
	{
		this.curManager = curManager;
	}
	
	public void setReactorManager(SecurityManager reactorManager)
	{
		this.reactorManager = reactorManager;
	}
	
	
	public Connection getConnection()
	{
		return ((H2Frame)dataframe).getBuilder().getConnection();
	}
	
	public void setConsole()
	{
		this.System = new Console();
	}

	public BaseJavaReactor(ITableDataFrame frame)
	{
		// empty constructor creates a frame
		dataframe = frame;
	}
	
	// set a variable
	// use this variable if it is needed on the next call
	public void storeVariable(String varName, Object value)
	{
		pkql.setVariableValue(varName, value);
	}
	

	// get the variable back that was set
	public Object getVariable(String varName)
	{
		return pkql.getVariableValue(varName);
	}
	
	// refresh the front end
	// use this call to indicate that you have manipulated the frame or have worked in terms of creating a newer frame
	public void refresh()
	{
		frameChanged = true;
		dataframe.updateDataId();
	}

	public void setPKQLRunner(PKQLRunner pkql)
	{
		this.pkql = pkql;
	}
	// couple of things I need here
	// access to the engines
	public IEngine getEngine(String engineName)
	{
		return null;
	}
	
	// sets the data frame
	public void setDataFrame(ITableDataFrame dataFrame)
	{
		this.dataframe = dataFrame;
	}
	
	public void runPKQL(String pkqlString)
	{
		// this will run PKQL
		System.out.println("Running pkql.. " + pkqlString);
		pkql.runPKQL(pkqlString, dataframe);
		
		myStore.put("RESPONSE", System.out.output);
	}
		
	public void preProcess()
	{
		
	}
	
	
	public void postProcess()
	{
		
	}

	public void filterNode(String columnHeader, String [] instances)
	{
		List <Object> values = new Vector();
		for(int instanceIndex = 0;instanceIndex < instances.length;values.add(instances[instanceIndex]), instanceIndex++);
		dataframe.filter(columnHeader, values);
	}

	public void filterNode(String columnHeader, String value)
	{
		List <Object> values = new Vector();
		values.add(value);
		dataframe.filter(columnHeader, values);
	}

	public void filterNode(String columnHeader, List <Object> instances)
	{
		dataframe.filter(columnHeader, instances);		
	}

	public void runGremlin()
	{
		
	}
	
	@Override
	public String[] getParams() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void set(String key, Object value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addReplacer(String pattern, Object value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String[] getValues2Sync(String childName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeReplacer(String pattern) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object getValue(String key) {
		// TODO Auto-generated method stub
		// return the key for now
		return myStore.get(key);
	}

	@Override
	public void put(String key, Object value) {
		// TODO Auto-generated method stub
		myStore.put(key, value);
		
	}

	public void degree(String type, String data)
	{
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame)
		{
			Object degree = ((TinkerFrame)dataframe).degree(type, data);
			String output = "Degrees for  " + data + ":" + degree;
			System.out.println(output);
		}
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	public void removeNode(String type, String data) {
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame)
		{
			List<Object> removeList = new Vector<Object>();
			removeList.add(data);
			((TinkerFrame)dataframe).remove(type, removeList);
			String output = "Removed nodes for  " + data + " with values " + removeList;
			System.out.println(output);
			dataframe.updateDataId();
		}
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	public void eigen(String type, String data)
	{
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame)
		{
			Object degree = ((TinkerFrame)dataframe).eigen(type, data);
			String output = "Eigen for  " + data + ":" +degree;
			System.out.println(output);
		}
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	public void isOrphan(String type, String data)
	{
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame)
		{
			boolean orphan = ((TinkerFrame)dataframe).isOrphan(type, data);
			String output = data + "  Orphan? " + orphan;
			System.out.println(output);
		}		
		java.lang.System.setSecurityManager(reactorManager);
	}	
}
