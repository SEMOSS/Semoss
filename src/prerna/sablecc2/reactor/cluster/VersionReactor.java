package prerna.sablecc2.reactor.cluster;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;

public class VersionReactor extends AbstractReactor {

	private static Map<String, String> versionMap;
	private static final String VER_PATH = DIHelper.getInstance().getProperty("BaseFolder") + System.getProperty("file.separator") + "ver.txt";
	
	@Override
	public NounMetadata execute() {
		return new NounMetadata(getVersionMap(), PixelDataType.MAP, PixelOperationType.VERSION);
	}
	
	public static Map<String, String> getVersionMap() {
		Properties props = new Properties();
		try (InputStream in = new FileInputStream(VER_PATH)) {
			props.load(in);
		} catch (IOException e) {
			NounMetadata noun = new NounMetadata("Failed to parse the version", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}
		
		if(versionMap == null) {
			versionMap = new HashMap<String, String>();
			versionMap.put("version", props.getProperty("version"));
			versionMap.put("datetime", props.getProperty("datetime"));
		}
		
		return versionMap;
	}

}
