package prerna.sablecc2.reactor.insights;

import java.util.List;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.ReactorFactory;
import prerna.util.Utility;

public class ReloadInsightClassesReactor extends AbstractReactor {

	public ReloadInsightClassesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		if(!Utility.isValidJava()) {
			throw new IllegalArgumentException("This operation is not available (tools.jar not available)");
		}

		organizeKeys();
		String engineId = insight.getRdbmsId();
		if(this.keyValue.get(this.keysToGet[0]) != null) {
			engineId = this.keyValue.get(this.keysToGet[0]);
		}

		ReactorFactory.recompile(engineId);
		List<String> queriedEngines = insight.getQueriedEngines();
		for(int engineIndex = 0; engineIndex < queriedEngines.size(); engineIndex++) {
			ReactorFactory.recompile(insight.getQueriedEngines().get(engineIndex));
		}
		
		return new NounMetadata("Recompile Completed", PixelDataType.CONST_STRING);
	}
}
