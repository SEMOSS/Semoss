package prerna.playground.reactors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetPlaygroundMessagesReactor extends AbstractReactor {

	private static final Gson gson = new Gson();

	public GetPlaygroundMessagesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.SORT.getKey(), };
		this.keyRequired = new int[] { 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		/**
		 * Organize reactor inputs
		 */
		organizeKeys();
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String limitStr = this.keyValue.get(ReactorKeysEnum.LIMIT.getKey());
		Integer limit = -1;
		String offsetStr = this.keyValue.get(ReactorKeysEnum.OFFSET.getKey());
		Integer offset = -1;
		String dateSortStr = this.keyValue.get(ReactorKeysEnum.SORT.getKey());
		String dateSort = "ASC";

		/**
		 * Get user information
		 */
		User user = this.insight.getUser();
		if (user == null)
			throw new IllegalArgumentException("You are not properly logged in");
		String userId = user.getPrimaryLoginToken().getId();

		/**
		 * Check whether the room is valid for the user. If not, error is thrown.
		 */
		ModelInferenceLogsUtils.validUserRoom(roomId, userId);

		/**
		 * Parse limit, offset and sort keys
		 */
		if (limitStr != null && !limitStr.isEmpty() && (offsetStr != null && !offsetStr.isEmpty())) {
			try {
				limit = Integer.parseInt(limitStr);
				offset = Integer.parseInt(offsetStr);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Invalid value for limit or offset passed");
			}
		}
		if (dateSortStr != null && dateSortStr.equals("DESC"))
			dateSort = "DESC";

		/**
		 * Convert each message to output map for return
		 */
		List<Map<String, Object>> outputMap = new ArrayList<>();

		/**
		 * Get room object (will load or fetch as needed) and convert messages to new
		 * format from legacy
		 */
		Room room;
		try {
			room = RoomUtils.getOrLoadRoom(roomId, this.insight);
		} catch (Exception e) {
			/**
			 * Returning empty map as that is the closet to the old design of empty array
			 * from db
			 */
			return new NounMetadata(outputMap, PixelDataType.VECTOR);
		}

		/**
		 * Filter/slice results without altering room message object
		 */
		List<AbstractMessage> page = RoomUtils.getPagedMessages(room.getMessages(), dateSort, offset, limit);

		/**
		 * Add messages to list
		 */
		for (AbstractMessage m : page) {
			if(m.getMessageType() == MessageType.RESPONSE_TOOL) {
				MessageUtils.updateToolResponseWithProjectMeta((ResponseMessage) m);
			}
			outputMap.add(jsonToMap(MessageUtils.toJson(m)));
		}

		return new NounMetadata(outputMap, PixelDataType.VECTOR);
	}

	/**
	 * Converts a JSON object string to a Map<String, Object>
	 * 
	 * @param json The JSON string (must be a JSON object: { ... })
	 * @return The parsed Map
	 */
	public static Map<String, Object> jsonToMap(String json) {
		if (json == null || json.trim().isEmpty() || !json.trim().startsWith("{")) {
			throw new IllegalArgumentException("Input must be a valid JSON object string.");
		}
		return gson.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
	}

}
