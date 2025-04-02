package prerna.reactor.app;

import prerna.reactor.utils.ExportProjectAppReactor;
import prerna.sablecc2.om.ReactorKeysEnum;

public class ExportAppReactor extends ExportProjectAppReactor {
	
	/**
	 * 
	 * @param projectNameAndId
	 * @return
	 */
	@Override
	protected String getFileName(String projectNameAndId) {
		return projectNameAndId + "_app.smss-app";
	}
	
	@Override
	public String getReactorDescription() {
	    return "Export an app as a single .smss-app file";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if(key.equals(ReactorKeysEnum.PROJECT.getKey())) {
	        return "This is a required value containing the id of the app that is being exported";
	    }
	    return super.getDescriptionForKey(key);
	}
	
}
