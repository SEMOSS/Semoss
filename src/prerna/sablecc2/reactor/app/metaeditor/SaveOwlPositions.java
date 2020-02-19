package prerna.sablecc2.reactor.app.metaeditor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.api.IEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class SaveOwlPositions extends AbstractReactor {

	private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
	
	public static final String FILE_NAME = "positions.json";
	
	public SaveOwlPositions() {
		this.keysToGet = new String[] {ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.POSITION_MAP.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		String appId = getAppId();
		if(appId == null) {
			throw new IllegalArgumentException("Must pass in the app id");
		}
		// run security tests + alias replacement
		appId = testAppId(appId, true);
		Map<String, Object> positions = getPositionMap();
		if(positions == null || positions.isEmpty()) {
			throw new IllegalArgumentException("Must pass in the valid position map");
		}
		
		// write the json file in the app folder
		// just put it in the same location as the OWL
		IEngine app = Utility.getEngine(appId);
		String owlFileLocation = app.getOWL();
		// put in same location
		File owlF = new File(owlFileLocation);
		String baseFolder = owlF.getParent();
		String positionJson = baseFolder + DIR_SEPARATOR + SaveOwlPositions.FILE_NAME;
		File positionFile = new File(positionJson);
		
		FileWriter writer = null;
		try {
			writer = new FileWriter(positionFile);
			GSON.toJson(positions, writer);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	
	private String getAppId() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs != null && grs.isEmpty()) {
			List<NounMetadata> strings = grs.getNounsOfType(PixelDataType.CONST_STRING);
			if(strings != null && !strings.isEmpty()) {
				return (String) strings.get(0).getValue();
			}
		}
		
		// check is passed as direct input
		List<NounMetadata> strings = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
		if(strings != null && !strings.isEmpty()) {
			return (String) strings.get(0).getValue();
		}
		
		return null;
	}
	
	private Map<String, Object> getPositionMap() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if(grs != null && grs.isEmpty()) {
			List<NounMetadata> maps = grs.getNounsOfType(PixelDataType.MAP);
			if(maps != null && !maps.isEmpty()) {
				return (Map<String, Object>) maps.get(0).getValue();
			}
		}
		
		// check is passed as direct input
		List<NounMetadata> maps = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(maps != null && !maps.isEmpty()) {
			return (Map<String, Object>) maps.get(0).getValue();
		}
		
		return null;
	}

}
