package prerna.sablecc2.reactor.cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.collections4.map.HashedMap;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class VersionReactor extends AbstractReactor {

	private static Map<String, String> versionMap;
	private static final String VER_PATH = DIHelper.getInstance().getProperty("BaseFolder") + java.nio.file.FileSystems.getDefault().getSeparator() + "ver.txt";
	public static String VERSION_KEY = "version";
	public static String DATETIME_KEY = "datetime";
	
	
	public VersionReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.RELOAD.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String reloadStr = this.keyValue.get(this.keysToGet[0]);
		boolean reload = reloadStr != null && Boolean.parseBoolean(reloadStr);
		return new NounMetadata(getVersionMap(reload), PixelDataType.MAP, PixelOperationType.VERSION);
	}
	
	public static Map<String, String> getVersionMap(boolean reload) {
		if(reload || VersionReactor.versionMap == null) {
			synchronized(VersionReactor.class) {
				if(VersionReactor.versionMap != null) {
					return VersionReactor.versionMap;
				}
				
				Properties props = Utility.loadProperties(VER_PATH);
				if(props == null) {
					NounMetadata noun = new NounMetadata("Failed to parse the version", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
					SemossPixelException err = new SemossPixelException(noun);
					err.setContinueThreadOfExecution(false);
					throw err;
				}
				
				Map<String, String> tempMap = new HashMap<>();
				tempMap.put(VERSION_KEY, props.getProperty(VERSION_KEY));
				tempMap.put(DATETIME_KEY, props.getProperty(DATETIME_KEY));
				VersionReactor.versionMap = new HashedMap<>(tempMap);
			}
		}
		
		return VersionReactor.versionMap;
	}

}
