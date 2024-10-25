package prerna.reactor.prompt;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityPromptUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetPromptReactor extends AbstractReactor {
	
	public GetPromptReactor() {
		this.keysToGet = new String[] { 
				ReactorKeysEnum.PROMPT_ID.getKey()
			};
	}

	@Override
	public NounMetadata execute() {
organizeKeys();
		
		String userId = this.insight.getUserId();
		if (userId == null || userId.isEmpty()) {
			throw new IllegalArgumentException("User is not properly logged in.");
		}
		String promptID = this.keyValue.get(ReactorKeysEnum.PROMPT_ID.getKey());
		if(promptID == null || promptID.isEmpty()) {
			throw new IllegalArgumentException("PROMPT ID must be passed in to get details for a specific prompt");
		}
		
		Map<String, Object> promptDetails = SecurityPromptUtils.getPrompt(promptID);
		NounMetadata nm = new NounMetadata(promptDetails, PixelDataType.MAP);
		return nm;
	}

}
