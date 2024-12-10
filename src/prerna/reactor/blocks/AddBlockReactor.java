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

public class AddBlockReactor extends AbstractReactor {

	public AddBlockReactor() {
		this.keysToGet =  new String[] {ReactorKeysEnum.DATA_TYPE_MAP.getKey()};
		this.keyRequired = new int[] {1};
	}

	@Override
	public NounMetadata execute() {
		
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed in to delete a block", PixelDataType.CONST_STRING,
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
		BlocksThemeUtils.addBlock(blockDetails);
		NounMetadata nm = new NounMetadata(true, PixelDataType.BOOLEAN);
		return nm;
	}
	
	private Map<String, Object> getBlockDetails() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.MAP.getKey());
		if(grs != null && !grs.isEmpty()) {
			List<NounMetadata> mapNouns = grs.getNounsOfType(PixelDataType.MAP);
			if(mapNouns != null && !mapNouns.isEmpty()) {
				return (Map<String, Object>) mapNouns.get(0).getValue();
				}
		}
		
		List<NounMetadata> mapNouns = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(mapNouns != null && !mapNouns.isEmpty()) {
			return (Map<String, Object>) mapNouns.get(0).getValue();
		}

		throw new NullPointerException("Must define the prompt to store it correctly");
	}

}
