package prerna.sablecc2.reactor.database.metaeditor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.database.upload.UploadInputUtility;
import prerna.util.Utility;

public class SaveOwlPositionsReactor extends AbstractReactor {

	private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
	
	public SaveOwlPositionsReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.POSITION_MAP.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		String databaseId = UploadInputUtility.getDatabaseNameOrId(this.store);
		if(databaseId == null) {
			throw new IllegalArgumentException("Must pass in the database id");
		}
		// run security tests + alias replacement
		databaseId = testDatabaseId(databaseId, true);
		Map<String, Object> positions = getPositionMap();
		if(positions == null || positions.isEmpty()) {
			throw new IllegalArgumentException("Must pass in the valid position map");
		}
		
		// write the json file in the database folder
		// just put it in the same location as the OWL
		IEngine database = Utility.getEngine(databaseId);
		ClusterUtil.reactorPullOwl(databaseId);
		File positionFile = database.getOwlPositionFile();
		
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
		ClusterUtil.reactorPushOwl(databaseId);

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	
	private Map<String, Object> getPositionMap() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if(grs != null && !grs.isEmpty()) {
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
