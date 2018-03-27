package prerna.sablecc2.reactor.legacy.playsheets;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.om.OldInsight;
import prerna.om.SEMOSSParam;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class GetPlaysheetParamsReactor extends AbstractReactor {

	public GetPlaysheetParamsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.ID.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String app = this.keyValue.get(this.keysToGet[0]);
		String insightId = this.keyValue.get(this.keysToGet[1]);
		IEngine engine = Utility.getEngine(app);
		Insight in = engine.getInsight(insightId).get(0);
		Hashtable outputHash = new Hashtable<String, Hashtable>();
		outputHash.put("result", in.getRdbmsId());
		if (in.isOldInsight()) {
			Vector<SEMOSSParam> paramVector = ((OldInsight) in).getInsightParameters();
			Hashtable optionsHash = new Hashtable();
			Hashtable paramsHash = new Hashtable();
			for (int paramIndex = 0; paramIndex < paramVector.size(); paramIndex++) {
				SEMOSSParam param = paramVector.elementAt(paramIndex);
				if (param.isDepends().equalsIgnoreCase("false")) {
					Vector<Object> vals = engine.getParamOptions(param.getParamID());
					Set<Object> uniqueVals = new HashSet<Object>(vals);
					optionsHash.put(param.getName(), uniqueVals);
				} else {
					optionsHash.put(param.getName(), "");
				}
				paramsHash.put(param.getName(), param);
			}
			outputHash.put("options", optionsHash);
			outputHash.put("params", paramsHash);
		}
		return new NounMetadata(outputHash, PixelDataType.MAP, PixelOperationType.PLAYSHEET_PARAMS);
	}

}
