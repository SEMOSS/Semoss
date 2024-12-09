package prerna.reactor.blocks;

import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.theme.BlocksThemeUtils;


public class EditBlockReactor extends AbstractReactor {

	public EditBlockReactor() {
		this.keysToGet =  new String[] {ReactorKeysEnum.MAP.getKey(), "tableName"};
		this.keyRequired = new int[] {1, 1};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Map<String, Object> blockDetails = getBlockDetails();
		String tableName = this.keyValue.get("tableName");
		boolean done = BlocksThemeUtils.editBlock(blockDetails, tableName);
		NounMetadata nm = new NounMetadata(done, PixelDataType.BOOLEAN);
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
