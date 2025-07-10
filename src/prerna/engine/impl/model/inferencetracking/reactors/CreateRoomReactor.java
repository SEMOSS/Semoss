package prerna.engine.impl.model.inferencetracking.reactors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.reactor.AbstractReactor;
import prerna.reactor.insights.AbstractInsightReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

public class CreateRoomReactor extends AbstractReactor {
	
	public CreateRoomReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.NAME.getKey(), ReactorKeysEnum.VECTORDB.getKey(), ReactorKeysEnum.FUNCTION.getKey(), ReactorKeysEnum.FUNCTION.getKey(), ReactorKeysEnum.WORKSPACE_ID.getKey()};
		this.keyRequired = new int [] {0,0,0,0,0};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String roomName = this.keyValue.get(ReactorKeysEnum.NAME.getKey());
		String context = this.keyValue.get(ReactorKeysEnum.CONTEXT.getKey());
		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());
		
		Map<String, Object> options = null;
		
		if (workspaceId == null) {
			List<String> vectorDbs = getVectorDbs();
			List<String> tools = getTools();
			if (!tools.isEmpty() || !vectorDbs.isEmpty()) {
				options = new HashMap<>();
				if (!tools.isEmpty()) {
					options.put("tools", tools);
				}
				if (!vectorDbs.isEmpty()) {
					options.put("vectorDbs", vectorDbs);
				}
			}
		}
		
		Room room = RoomUtils.createRoomIfNotExists(UUID.randomUUID().toString(), insight, null, roomName, options, context);

		return new NounMetadata(room.getId(), PixelDataType.CONST_STRING);
	}
	
	private List<String> getVectorDbs() {
        List<String> inputStrings = new ArrayList<>();
        GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.VECTORDB.getKey());
        if (grs != null && !grs.isEmpty()) {
            int size = grs.size();
            for (int i = 0; i < size; i++) inputStrings.add(grs.get(i).toString());
            return inputStrings;
        }
        int size = this.curRow.size();
        for (int i = 0; i < size; i++) inputStrings.add(this.curRow.get(i).toString());
        return inputStrings;
    }
	
	private List<String> getTools() {
        List<String> inputStrings = new ArrayList<>();
        GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.FUNCTION.getKey());
        if (grs != null && !grs.isEmpty()) {
            int size = grs.size();
            for (int i = 0; i < size; i++) inputStrings.add(grs.get(i).toString());
            return inputStrings;
        }
        int size = this.curRow.size();
        for (int i = 0; i < size; i++) inputStrings.add(this.curRow.get(i).toString());
        return inputStrings;
    }
}
