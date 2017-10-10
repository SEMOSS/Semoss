package prerna.sablecc2.reactor.panel;

import java.util.Set;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetInsightPanelsReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		Set<String> panelIds = this.insight.getInsightPanels().keySet();
		return new NounMetadata(panelIds, PixelDataType.VECTOR, PixelOperationType.PANEL);
	}
}
