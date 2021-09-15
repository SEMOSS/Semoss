package prerna.sablecc2.reactor.algorithms.xray;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

/**
 * Get X-ray configuration stored in LocalMaster by specifying the filename 
 */
public class GetXrayConfigFileReactor extends AbstractReactor {

	public GetXrayConfigFileReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.FILE_NAME.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String configFileID = this.keyValue.get(this.keysToGet[0]);
		if(configFileID == null) {
			throw new IllegalArgumentException("Need to define " + ReactorKeysEnum.FILE_NAME.getKey());
		}
		String configFile = MasterDatabaseUtility.getXrayConfigFile(configFileID);
		return new NounMetadata(configFile, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CODE_EXECUTION);
	}

}
