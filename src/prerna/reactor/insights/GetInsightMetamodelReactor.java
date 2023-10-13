package prerna.reactor.insights;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetInsightMetamodelReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GetInsightMetamodelReactor.class);
	
	public GetInsightMetamodelReactor() {
		this.keysToGet = new String[]{
				ReactorKeysEnum.PROJECT.getKey(),
				ReactorKeysEnum.ID.getKey()
				};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String id = this.keyValue.get(this.keysToGet[1]);
		
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if(!SecurityInsightUtils.userCanViewInsight(this.insight.getUser(), projectId, id)) {
			throw new IllegalArgumentException("Insight does not exist or user does not have permission to view this insight");
		}
		
		List<Object[]> insightFrames = SecurityInsightUtils.getInsightFrames(projectId, id);
		
		Map<String, List<Object[]>> groupedByFrame = groupConcepts(insightFrames);
		
		Map<String, Object> result = new HashMap<>();
		for (String cn : groupedByFrame.keySet()) {
			List<Object[]> objs = groupedByFrame.get(cn);
			Map<String, Object> frameMetamodel = buildResponsePerFrame(cn, objs);
			result.put(cn, frameMetamodel);
		}
		
		return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.INSIGHT_METAMODEL);
	}
	
	private Map<String, Object> buildResponsePerFrame(String cn, List<Object[]> o) {
		Map<String, Object> results = new HashMap<>();
		List<String> edges = new ArrayList<>();
		results.put("edges", edges);

		Map<String, Object> nodes = new HashMap<>();
		List<String> propSet = new ArrayList<>();
		nodes.put("propSet", propSet);
		nodes.put("conceptualName", cn);
				
		Map<String, String> additionalDataTypes = new HashMap<>();
		Map<String, String> dataTypes = new HashMap<>();
		
		results.put("propSet", propSet);
		results.put("additionalDataTypes", additionalDataTypes);
		results.put("dataTypes", dataTypes);
		
		for (Object[] ob : o) {
			String alias = ob[2].toString();
			String colName = ob[4].toString();
			String colType = ob[5].toString();
			String sadtl = null;
			
			String unique = cn + "__" + colName;
			
			Object adtl = ob[6];
			if (adtl != null) {
				sadtl = adtl.toString();
				additionalDataTypes.put(unique, sadtl);
			}
			
			propSet.add(colName);
			dataTypes.put(unique, colType);
		}
		
		return results;
	}
	
	private Map<String, List<Object[]>> groupConcepts(List<Object[]> insightFrames) {
		Map<String, List<Object[]>> groupedByFrame = new HashMap<>();
		for (Object[] ifs : insightFrames) {
			Object cn = ifs[2];
			if (cn == null) {
				continue;
			}
			
			String scn = cn.toString();
			
			List<Object[]> curr;
			if (groupedByFrame.containsKey(scn)) {
				curr = groupedByFrame.get(scn);
			} else {
				curr = new ArrayList<>();
			}
			curr.add(ifs);
			groupedByFrame.put(scn, curr);
		}
		
		return groupedByFrame;
	}
}
