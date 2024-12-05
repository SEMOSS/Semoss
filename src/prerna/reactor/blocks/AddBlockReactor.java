package prerna.reactor.blocks;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.theme.BlocksThemeUtils;
import prerna.theme.blockTemplateTableRow;

public class AddBlockReactor extends AbstractReactor {

	public AddBlockReactor() {
		this.keysToGet =  new String[] {ReactorKeysEnum.DATA_TYPE_MAP.getKey()};
		this.keyRequired = new int[] {1};
	}

	@Override
	public NounMetadata execute() {

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
