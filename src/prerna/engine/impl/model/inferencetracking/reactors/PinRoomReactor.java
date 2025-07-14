package prerna.engine.impl.model.inferencetracking.reactors;

import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PinRoomReactor extends AbstractReactor {
	
	private static final String FAVORITE = "favorite";
	
	public PinRoomReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ROOM_ID.getKey(), FAVORITE};
		this.keyRequired = new int [] {1, 1};
	}
	

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		boolean pinned = Boolean.parseBoolean(this.keyValue.get(FAVORITE));
		
		boolean result = ModelInferenceLogsUtils.doSetRoomToPinned(insight.getUser().getPrimaryLoginToken().getId(), roomId, pinned);
		
		return new NounMetadata(result, PixelDataType.BOOLEAN);
	}

}
