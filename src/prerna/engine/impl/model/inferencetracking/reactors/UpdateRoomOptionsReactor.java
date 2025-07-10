package prerna.engine.impl.model.inferencetracking.reactors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import prerna.sablecc2.om.GenRowStruct;

public class UpdateRoomOptionsReactor extends AbstractReactor {
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(UpdateRoomOptionsReactor.class);

	// consider changing to just take in an options param so that diff apps can do what they want
	public UpdateRoomOptionsReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.VECTORDB.getKey(), ReactorKeysEnum.FUNCTION.getKey()};
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}

		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		Map<String, Object> options = null;
		
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

		ModelInferenceLogsUtils.setRoomOptions(roomId, user.getPrimaryLoginToken().getId(), options);
		return new NounMetadata(options, PixelDataType.MAP);
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