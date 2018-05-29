package prerna.sablecc2.reactor.task.lambda.flatmap;

import java.util.List;
import java.util.Map;

import prerna.auth.User2;

public abstract class AbstractFlatMapLambda implements IFlatMapLambda {

	protected List<Map<String, Object>> headerInfo;
	protected User2 user;
	
	@Override
	public List<Map<String, Object>> getModifiedHeaderInfo() {
		return this.headerInfo;
	}
	
	@Override
	public void setUser2(User2 user) {
		this.user = user;
	}
	
	public void setParams(Map params)
	{
		
	}

}
