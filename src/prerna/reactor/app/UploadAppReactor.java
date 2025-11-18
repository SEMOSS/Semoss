package prerna.reactor.app;

import prerna.reactor.project.UploadProjectAppReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UploadAppReactor extends UploadProjectAppReactor {

	@Override
	protected boolean deleteIfExisting() {
		String modeKey = this.keyValue.getOrDefault(MODE_KEY, REPLACE_MODE).trim();
		if (CREATE_MODE.equalsIgnoreCase(modeKey)) {
			return false;
		}
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
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "This is a required value containing the relative file path of the single .smss-app file to be imported";
		} else if (key.equals(ReactorKeysEnum.SPACE.getKey())) {
			return "This is an optional field to determine the space in which the relative file path exists (user project space, current insight space, project id space).";
		} else if (key.equals(ReactorKeysEnum.GLOBAL.getKey())) {
			return "This is a required value to determine if the app is public or private";
		} else if (key.equals(MODE_KEY)) {
			return """
					Optional paramter that is either 'create' or 'replace'.
					'create' is the default and will break if the app id already exists.
					'replace' will replace if the app id exist but user must be an owner of the app.
					Default is 'replace' if no value is passed in.
					""";
		}
		return super.getDescriptionForKey(key);
	}
}
