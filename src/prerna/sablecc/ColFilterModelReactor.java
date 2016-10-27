package prerna.sablecc;

import java.util.Iterator;
import java.util.Vector;

import prerna.sablecc.meta.ColAddMetadata;
import prerna.sablecc.meta.ColFilterModelMetadata;
import prerna.sablecc.meta.DataframeHeaderMetadata;
import prerna.sablecc.meta.IPkqlMetadata;

public class ColFilterModelReactor extends AbstractReactor{
	//col.filterModel(c:col, "word", {limit:20, offset:10});
	public ColFilterModelReactor() {
		String [] thisReacts = {PKQLEnum.COL_DEF, PKQLEnum.WORD_OR_NUM, PKQLEnum.MATH_PARAM};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.COL_FILTER_MODEL;
	}

	@Override
	public Iterator process() {
		// implemented in H2ColFilterModelReactor and TinkerColFilterModelReactor
		return null;
	}
	
	@Override
	public IPkqlMetadata getPkqlMetadata() {
		Vector<String> colVector = (Vector) myStore.get(PKQLEnum.COL_DEF);
		String col = (String) colVector.get(0);
		String word = (String) myStore.get("filterWord");
		if(word != null) {
		word = word.substring(1,word.length()-2);
		}
		ColFilterModelMetadata metadata = new ColFilterModelMetadata(col);
		metadata.setPkqlStr((String) myStore.get(PKQLEnum.COL_FILTER_MODEL));
		return metadata;
	}
}
