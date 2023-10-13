package prerna.reactor.task.lambda.map;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;

public abstract class AbstractMapLambda implements IMapLambda {

	protected Map params = new HashMap();
	protected List<Map<String, Object>> headerInfo;
	protected User user;
	
	@Override
	public List<Map<String, Object>> getModifiedHeaderInfo() {
		return this.headerInfo;
	}
	
	@Override
	public void setUser(User user) {
		this.user = user;
	}

	@Override
	public void setParams(Map params) {
		this.params = params;
	}
}
