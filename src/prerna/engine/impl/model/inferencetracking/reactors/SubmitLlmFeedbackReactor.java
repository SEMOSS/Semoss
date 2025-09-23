package prerna.engine.impl.model.inferencetracking.reactors;

import java.util.List;

import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.impl.model.MessageFeedback;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SubmitLlmFeedbackReactor extends AbstractReactor {
	
    public SubmitLlmFeedbackReactor() {
        this.keysToGet = new String[] {"roomId", "messageId", "feedbackText", "rating"};
        this.keyRequired = new int[] {1, 1, 0, 1};
    }

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		String roomId = this.keyValue.get(this.keysToGet[0]);
		String messageId = this.keyValue.get(this.keysToGet[1]);
		String feedbackText = this.keyValue.get(this.keysToGet[2]);
		Boolean rating = null;
	    if ("true".equalsIgnoreCase(keyValue.get(keysToGet[3]))) {
	      rating = Boolean.TRUE;
	    } else if ("false".equalsIgnoreCase(keyValue.get(keysToGet[3]))) {
	      rating = Boolean.FALSE;
	    }
		
		User user = this.insight.getUser();
        if (user == null) {
            throw new IllegalArgumentException("You are not properly logged in");
        }
        
//        TODO: perhaps switch to just verifying from the room object instead?
        boolean output = ModelInferenceLogsUtils.userIsMessageAuthor(user.getPrimaryLoginToken().getId(), messageId);
        if (!output) {
        	throw new SemossPixelException("User is not the author of this message and cannot provide feedback");
        }
        
//        Load room
        Room room = RoomUtils.getOrLoadRoom(roomId, insight);
        
//        Get messages
        List<AbstractMessage> messagesList = room.getMessages();
        
//        Create feedback object
        MessageFeedback feedback = new MessageFeedback(messageId, MessageType.RESPONSE_TEXT, feedbackText, rating);
        
//        Add feedback to message
        messagesList.parallelStream().forEach(msg -> {
        	if (msg.getMessageType().equals(feedback.getMessageType()) && msg.getMessageId().equals(feedback.getMessageId())) {
        		msg.setFeedback(feedback);
        	}
        });
        
//        Flush messages to db
        ModelInferenceLogsUtils.llm2_updateRoomMessages(room.getId(), insight.getUser().getPrimaryLoginToken().getId(), room.getMessagesAsString());
        
//        Now can add to the feedback table
        if (rating != null) {
        	ModelInferenceLogsUtils.recordFeedback(feedback);
        } else {
        	ModelInferenceLogsUtils.removeFeedback(messageId);
        }
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("messageId")) {
			return "The unique identififer for the I/O betweeen a user and the LLM response";
		} else if (key.equals("feedbackText")) {
			return "Additional feedback in the form of text to decribe the issue/benefits of the response";
		} else if (key.equals("rating")) {
			return "true/false value to indicate if the reponse was helpful or not, or null if not parsed correctly/null";
		} else if (key.equals("roomId")) {
			return "The room into which to add this feedback";
		}
		return super.getDescriptionForKey(key);
	}
}
