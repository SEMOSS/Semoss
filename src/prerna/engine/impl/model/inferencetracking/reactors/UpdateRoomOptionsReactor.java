package prerna.engine.impl.model.inferencetracking.reactors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import java.util.List;
import java.util.Map;
import prerna.sablecc2.om.GenRowStruct;

public class UpdateRoomOptionsReactor extends AbstractReactor {
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(UpdateRoomOptionsReactor.class);

	public UpdateRoomOptionsReactor() {
		this.keysToGet = new String[] { "roomId", "roomOptions" };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}

		String roomId = this.keyValue.get(this.keysToGet[0]);
		Map<String, Object> roomOptions = getRoomOptionsMap();

		boolean result = ModelInferenceLogsUtils.updateRoomOptions(roomId, user.getPrimaryLoginToken().getId(), roomOptions);
		return new NounMetadata(result, PixelDataType.BOOLEAN);
	}

	/**
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> getRoomOptionsMap() {
		GenRowStruct mapGrs = this.store.getNoun(keysToGet[1]);
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}
}