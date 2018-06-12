package prerna.sablecc2.reactor.task.lambda.map;

import java.util.List;
import java.util.Map;

import prerna.auth.User;

public abstract class AbstractMapLambda implements IMapLambda {

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

}
