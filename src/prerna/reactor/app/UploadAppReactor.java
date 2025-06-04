package prerna.reactor.app;

import prerna.reactor.project.UploadProjectAppReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UploadAppReactor extends UploadProjectAppReactor {
	
	@Override
	protected boolean deleteIfExisting() {
		return true;
	}
	
	@Override
	public NounMetadata execute() {
		return super.execute();
	}
	
	@Override
	public String getReactorDescription() {
	    return "Import an app from an exported .smss-app file";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if(key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
	        return "This is a required value containing the relative file path of the single .smss-app file to be imported";
	    } else if(key.equals(ReactorKeysEnum.SPACE.getKey())) {
	        return "This is an optional field to determine the space in which the relative file path exists (user project space, current insight space, project id space).";
	    } else if(key.equals(ReactorKeysEnum.GLOBAL.getKey())) {
	    	return "This is a required value to determine if the app is public or private";
	    }
	    return super.getDescriptionForKey(key);
	}
}
