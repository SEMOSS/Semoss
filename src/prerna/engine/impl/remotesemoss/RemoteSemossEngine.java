package prerna.engine.impl.remotesemoss;

import java.util.Vector;

import prerna.engine.impl.AbstractEngine;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

// TODO >>>timb: REST - either replace with rest remote or remove this
public class RemoteSemossEngine extends AbstractEngine {

	private String remoteAddress;
	
	public void openDB(String propFile) {
		this.baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		this.prop = Utility.loadProperties(propFile);
		
		// get id & name
		this.engineId = this.prop.getProperty(Constants.ENGINE);
		this.engineName = this.prop.getProperty(Constants.ENGINE_ALIAS);
		
		this.remoteAddress = this.prop.getProperty("REMOTE_ADDRESS");
	}
	
	@Override
	public Object execQuery(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vector<Insight> getInsight(String... questionIDs) {
		System.out.println("need to implement this in rest");
		return null;
	}
	
	
	@Override
	public void insertData(String query) {
		System.out.println("need to implement this in rest");
	}

	@Override
	public void removeData(String query) {
		System.out.println("need to implement this in rest");
		
	}

	@Override
	public void commit() {
		System.out.println("need to implement this in rest");
		
	}

	@Override
	public ENGINE_TYPE getEngineType() {
		return ENGINE_TYPE.REMOTE_SEMOSS;
	}
	
	//////////////////////////////////////////////////////
	
	// I am not sure this is really used much... 
	// need to check some reactors that touch the engines directly
	// like x-ray
	
	@Override
	public Vector<Object> getEntityOfType(String type) {
		// TODO Auto-generated method stub
		return null;
	}

}
