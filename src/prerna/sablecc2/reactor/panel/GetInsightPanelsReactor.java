package prerna.sablecc2.reactor.panel;

import java.util.Set;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.PkslOperationTypes;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetInsightPanelsReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		Set<String> panelIds = this.insight.getInsightPanels().keySet();
		return new NounMetadata(panelIds, PkslDataTypes.VECTOR, PkslOperationTypes.PANEL);
	}
}
