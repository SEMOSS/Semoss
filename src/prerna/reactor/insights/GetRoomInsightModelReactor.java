package prerna.reactor.insights;

import java.util.List;
import java.util.Map;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetRoomInsightModelReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
        String roomId = this.insight.getRoomId();
		if (roomId == null) {
            throw new IllegalArgumentException("Insight is not associated with any room");
        }
		String userId = this.insight.getUser().getPrimaryLoginToken().getId();
		Room room = ModelInferenceLogsUtils.getRoomById(roomId, userId);
		Map<String, Object> optionsMap = room.getOptionsMap();
        String modelId = null;
		if (optionsMap.containsKey("modelId"))
            modelId = (String) optionsMap.get("modelId");
        else {
            throw new IllegalArgumentException("No model associated with the room");
        }
        return new NounMetadata(modelId, PixelDataType.CONST_STRING);
	}
    
}
