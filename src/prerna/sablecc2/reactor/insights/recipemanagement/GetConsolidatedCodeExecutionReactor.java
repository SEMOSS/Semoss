package prerna.sablecc2.reactor.insights.recipemanagement;

import java.util.ArrayList;
import java.util.List;

import prerna.om.Pixel;
import prerna.om.PixelList;
import prerna.om.Variable.LANGUAGE;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetConsolidatedCodeExecutionReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		List<StringBuffer> consolidatedCode = new ArrayList<>();
		
		PixelList pList = this.insight.getPixelList();
		int size = pList.size();
		if(size == 0) {
			return new NounMetadata(consolidatedCode, PixelDataType.VECTOR);
		}
		
		StringBuffer buffer = new StringBuffer();
		// keep combining until we get to a point where we switch languages
		LANGUAGE prevLanguage = null;
		for(int i = 0; i < size; i++) {
			Pixel p = pList.get(i);
			if(p.isCodeExecution()) {
				if(prevLanguage == p.getLanguage()) {
					// combine into the same buffer
					buffer.append(p.getCodeExecuted()).append("\n");
				} else if(prevLanguage == null){
					consolidatedCode.add(buffer);
					buffer.append(p.getCodeExecuted()).append("\n");
					prevLanguage = p.getLanguage();
				} else {
					buffer = new StringBuffer();
					consolidatedCode.add(buffer);
					buffer.append(p.getCodeExecuted()).append("\n");
					prevLanguage = p.getLanguage();
				}
			}
		}
		
		List<String> retCode = new ArrayList<>();
		for(int i = 0; i < consolidatedCode.size(); i++) {
			retCode.add(consolidatedCode.get(i).toString());
		}
		
		return new NounMetadata(retCode, PixelDataType.VECTOR);
	}

}
