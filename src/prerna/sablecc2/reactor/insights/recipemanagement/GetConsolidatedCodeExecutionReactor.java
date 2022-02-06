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
		List<String> consolidatedCode = new ArrayList<>();
		
		PixelList pList = this.insight.getPixelList();
		int size = pList.size();
		if(size == 0) {
			return new NounMetadata(consolidatedCode, PixelDataType.VECTOR);
		}
		
		StringBuffer buffer = new StringBuffer();
		Pixel prevP = pList.get(0);
		boolean prevIsCode = prevP.isCodeExecution();
		LANGUAGE prevLanguage = prevP.getLanguage();
		boolean canCombine = prevIsCode;
		if(canCombine) {
			buffer.append(prevP.getCodeExecuted());
		}
		for(int i = 1; i < size; i++) {
			Pixel p = pList.get(i);
			canCombine = p.isCodeExecution() && 
					p.getLanguage() == prevLanguage;
			
			if(canCombine) {
				buffer.append("\n").append(p.getCodeExecuted());
			} else if(prevIsCode) {
				consolidatedCode.add(buffer.toString());
				buffer = new StringBuffer();
				if(p.isCodeExecution()) {
					buffer.append(p.getCodeExecuted());
				}
			} else if(p.isCodeExecution()) {
				buffer.append(p.getCodeExecuted());
			}
			
			prevP = p;
		}
		
		return new NounMetadata(consolidatedCode, PixelDataType.VECTOR);
	}

}
