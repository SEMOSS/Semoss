package prerna.reactor.insights;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CurrentVariablesReactor extends AbstractReactor{

	@Override
	public NounMetadata execute() {
		
		Map<String, Object> varMap = new HashMap<String, Object>();

		
		//lets get pixel vars first
		Set<PixelDataType> printableTypes = new HashSet<PixelDataType>();
		printableTypes.add(PixelDataType.CONST_DECIMAL);
		printableTypes.add(PixelDataType.CONST_INT);
		printableTypes.add(PixelDataType.CONST_STRING);
		printableTypes.add(PixelDataType.CONST_DATE);
		printableTypes.add(PixelDataType.CONST_TIMESTAMP);

		VarStore pixelVar = this.insight.getVarStore();
		Map<String, String> returnPixelMap = new HashMap<String, String>();

		Set<String> pixelkeys = pixelVar.getKeys();
		for(String key : pixelkeys){
			//if the key starts with a $ we are going to skip it ? double check. 
			if(key.charAt(0) == '$'){
				continue;
			}
			String value = "";
			if(printableTypes.contains(pixelVar.get(key).getNounType())){
				value = pixelVar.get(key).getValue().toString();
			} else{
				value=pixelVar.get(key).getNounType().toString();
			}
			returnPixelMap.put(key, value);
		}
		
		
		//guess we try R now
//		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(this.getLogger(this.getClass().getName()));
//		rJavaTranslator.
		varMap.put("PIXEL", returnPixelMap);
		return new NounMetadata(varMap, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

}
