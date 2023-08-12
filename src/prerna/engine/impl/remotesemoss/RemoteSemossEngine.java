package prerna.engine.impl.remotesemoss;

import java.util.Vector;

import prerna.engine.impl.AbstractDatabase;
import prerna.util.Constants;
import prerna.util.DIHelper;

// TODO >>>timb: REST - either replace with rest remote or remove this
public class RemoteSemossEngine extends AbstractDatabase {

	private String remoteAddress;
	
	public void openDB(String propFile) {
		this.baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		setSmssFilePath(propFile);
		
		// get id & name
		this.engineId = this.smssProp.getProperty(Constants.ENGINE);
		this.engineName = this.smssProp.getProperty(Constants.ENGINE_ALIAS);
		
		this.remoteAddress = this.smssProp.getProperty("REMOTE_ADDRESS");
	}
	
	@Override
	public Object execQuery(String query) {
		// TODO Auto-generated method stub
		return null;
	}

//	@Override
//	public Vector<Insight> getInsight(String... questionIDs) {
//		System.out.println("need to implement this in rest");
//		return null;
//	}
	
	
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
	public DATABASE_TYPE getDatabaseType() {
		return DATABASE_TYPE.REMOTE_SEMOSS;
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
