package prerna.sablecc2.reactor.insights;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.ReactorFactory;

public class ReloadInsightClassesReactor extends AbstractReactor {

	public ReloadInsightClassesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String idToRemove = insight.getRdbmsId();
		if(this.keyValue.get(this.keysToGet[0]) != null) {
			idToRemove = this.keyValue.get(this.keysToGet[0]);
		}

		ReactorFactory.recompile(idToRemove);
		
		//TODO:
		//TODO:
		//TODO:
		//TODO:
		//TODO:
		//TODO:
		//TODO:
		//TODO:
		//TODO:
		//TODO:
		//TODO:
//		List<String> queriedEngines = insight.getQueriedDatabaseIds();
//		for(int engineIndex = 0; engineIndex < queriedEngines.size(); engineIndex++) {
//			ReactorFactory.recompile(insight.getQueriedDatabaseIds().get(engineIndex));
//		}
		
		return new NounMetadata("Recompile Completed", PixelDataType.CONST_STRING);
	}
}
