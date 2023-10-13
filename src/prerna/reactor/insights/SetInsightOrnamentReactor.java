package prerna.reactor.insights;

import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetInsightOrnamentReactor extends AbstractReactor {

	/**
	 * Used to set insight level ornaments - e.g. client mode for the insight (no "bogger" on side menu)
	 */
	
	public SetInsightOrnamentReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.INSIGHT_ORNAMENT.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		Map<String, Object> inOrnament = (Map<String, Object>) this.curRow.get(0);
		this.insight.setInsightOrnament(inOrnament);
		return new NounMetadata(inOrnament, PixelDataType.MAP, PixelOperationType.INSIGHT_ORNAMENT);
	}

}
