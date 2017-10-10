package prerna.sablecc2.reactor.insights;

import java.util.Map;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;

public class SetInsightOrnamentReactor extends AbstractReactor {

	/**
	 * Used to set insight level ornaments - e.g. client mode for the insight (no "booger" on side menu)
	 */
	
	@Override
	public NounMetadata execute() {
		Map<String, Object> inOrnament = (Map<String, Object>) this.curRow.get(0);
		this.insight.setInsightOrnament(inOrnament);
		return new NounMetadata(inOrnament, PixelDataType.MAP, PixelOperationType.INSIGHT_ORNAMENT);
	}

}
