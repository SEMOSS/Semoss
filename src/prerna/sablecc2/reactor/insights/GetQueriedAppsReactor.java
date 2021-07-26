package prerna.sablecc2.reactor.insights;

import java.util.List;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetQueriedAppsReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		List<String> apps = this.insight.getQueriedEngines();
		if(this.insight.isSavedInsight()) {
			if(apps.contains(this.insight.getProjectId())) {
				apps.remove(this.insight.getProjectId());
			}
			apps.add(0, this.insight.getInsightId());
		}
		return new NounMetadata(apps, PixelDataType.CONST_STRING);
	}

}
