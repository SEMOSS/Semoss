package prerna.reactor.blocks;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.theme.BlocksThemeUtils;
import prerna.theme.ThemeDbTable;

public class AddBlockReactor extends AbstractReactor {

	public AddBlockReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.NAME.getKey(), ReactorKeysEnum.SECTION.getKey(), ReactorKeysEnum.JSON.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {

		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed in to add a block", PixelDataType.CONST_STRING,
					PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		if (AbstractSecurityUtils.anonymousUsersEnabled()) {
			if (this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
		}

		organizeKeys();
		Map<String, Object> blockDetails = getBlockDetails();
		String blockId = BlocksThemeUtils.addBlock(blockDetails);
		NounMetadata nm = new NounMetadata(blockId, PixelDataType.CONST_STRING);
		return nm;
	}

    // prepares map from inputs fields for use in block creation logic 
	private Map<String, Object> getBlockDetails() {
	    Map<String, Object> blockMap = new HashMap<>();
	        blockMap.put("name", keyValue.get(ReactorKeysEnum.NAME.getKey()));
	        blockMap.put("section",keyValue.get(ReactorKeysEnum.SECTION.getKey()));
	        blockMap.put("json",keyValue.get(ReactorKeysEnum.JSON.getKey()));
	       return blockMap;
	}

}
