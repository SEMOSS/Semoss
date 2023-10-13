package prerna.reactor.panel;

import java.util.Set;
import java.util.Vector;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class InsightPanelIds extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		Set<String> panelIds = this.insight.getInsightPanels().keySet();
		return new NounMetadata(new Vector<>(panelIds), PixelDataType.VECTOR, PixelOperationType.PANEL);
	}
	
	public String getName()
	{
		return "InsightPanelIds";
	}

}
