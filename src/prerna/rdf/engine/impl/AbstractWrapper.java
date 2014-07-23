package prerna.rdf.engine.impl;

import prerna.rdf.engine.api.IRemoteQueryable;

public class AbstractWrapper implements IRemoteQueryable{

	String ID = null;
	String api = null;
	boolean remote = false;
	
	@Override
	public void setRemoteID(String id) {
		// TODO Auto-generated method stub
		this.ID = id;
	}

	@Override
	public String getRemoteID() {
		// TODO Auto-generated method stub
		return this.ID;
	}

	@Override
	public void setRemoteAPI(String engine) {
		// TODO Auto-generated method stub
		this.api = engine;
	}

	@Override
	public String getRemoteAPI() {
		// TODO Auto-generated method stub
		return this.api;
	}
	
	public void setRemote(boolean remote)
	{
		this.remote = remote;
	}

}
