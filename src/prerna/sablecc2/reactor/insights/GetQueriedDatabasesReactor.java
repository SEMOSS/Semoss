package prerna.sablecc2.reactor.insights;

import java.util.Set;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetQueriedDatabasesReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		Set<String> databases = this.insight.getQueriedDatabaseIds();
		if(this.insight.isSavedInsight()) {
			if(databases.contains(this.insight.getProjectId())) {
				databases.remove(this.insight.getProjectId());
			}
			databases.add(this.insight.getInsightId());
		}
		return new NounMetadata(databases, PixelDataType.CONST_STRING);
	}

}
