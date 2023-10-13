package prerna.reactor.export;

import java.util.HashMap;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class FrameCacheReactor extends AbstractReactor {
	
	public FrameCacheReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME_CACHE.getKey() };
	}
	
	public NounMetadata execute() {
		// default this to use Python
		// if Python not present
		// try in R
		// default is R
		// reset it ?
		
		if(insight.getPragmap() == null) {
			insight.setPragmap(new HashMap());
		}
		this.insight.getPragmap().put("xCache", this.curRow.vector.get(0).getValue());
		
		boolean value = Boolean.parseBoolean(insight.getPragmap().get("xCache") + "");
		NounMetadata noun = new NounMetadata(value, PixelDataType.BOOLEAN, PixelOperationType.FRAME_CACHE);
		noun.addAdditionalReturn(getSuccess("Cache is now set to " + value));
		return noun;
	}
}
