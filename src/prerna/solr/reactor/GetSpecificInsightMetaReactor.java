package prerna.solr.reactor;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetSpecificInsightMetaReactor extends AbstractReactor {

	private static List<String> META_KEYS_LIST = new Vector<String>();
	static {
		META_KEYS_LIST.add("description");
		META_KEYS_LIST.add("tag");
	}
	
	public GetSpecificInsightMetaReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String rdbmsId = this.keyValue.get(this.keysToGet[1]);

		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if(!SecurityInsightUtils.userCanViewInsight(this.insight.getUser(), projectId, rdbmsId)) {
			NounMetadata noun = new NounMetadata("User does not have access to this insight", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}
		
		Map<String, Object> retMap = SecurityInsightUtils.getSpecificInsightMetadata(projectId, rdbmsId, META_KEYS_LIST);
		retMap.putIfAbsent("description", "");
		retMap.putIfAbsent("tags", new Vector<String>());
		// put in cacheable and cacheMinutes
		retMap.putAll(SecurityInsightUtils.getSpecificInsightCacheDetails(projectId, rdbmsId));
		retMap.put("global", SecurityInsightUtils.insightIsGlobal(projectId, rdbmsId));
		
		NounMetadata retNoun = new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
		return retNoun;
	}
}
