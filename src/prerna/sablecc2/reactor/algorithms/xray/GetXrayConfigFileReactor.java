package prerna.sablecc2.reactor.algorithms.xray;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetXrayConfigFileReactor extends AbstractReactor {
	public static final String CONFIG_FILE_ID = "configID";
	public GetXrayConfigFileReactor () {
		this.keysToGet = new String[]{};
	}
	@Override
	public NounMetadata execute() {
		String configFileID = getConfigFileID();
		String configFile = MasterDatabaseUtility.getXrayConfigFile(configFileID);
		ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
		return new NounMetadata(configFile, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CODE_EXECUTION);
	}

	private String getConfigFileID() {
		GenRowStruct grs = this.getCurRow();
		if (grs != null && !grs.isEmpty()) {
			String configFile = grs.getNoun(0).getValue() + "";
			if (configFile.length() > 0) {
				return configFile;
			}
		}
		throw new IllegalArgumentException("Need to define "+ CONFIG_FILE_ID);
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(CONFIG_FILE_ID)) {
			return "The id of the configuration file";

		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
