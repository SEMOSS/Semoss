package prerna.sablecc;

import java.util.List;
import java.util.Vector;

import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc.meta.MathPkqlMetadata;

public abstract class MathReactor extends AbstractReactor {

	/*
	 * Ughhhh... as of right now, this class is never actually used for anything
	 * It only serves for defining the constructor pieces which state
	 * what this reactor reacts to and setting the default pkql metadata
	 */
	
	// math routine to store for the explanation
	protected String mathRoutine;
	
	public MathReactor() {
		String[] thisReacts = { PKQLEnum.EXPR_TERM, PKQLEnum.FORMULA, PKQLEnum.DECIMAL, PKQLEnum.NUMBER, PKQLEnum.GROUP_BY,
				PKQLEnum.COL_DEF, PKQLEnum.MAP_OBJ};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.MATH_FUN;
	}

	public IPkqlMetadata getPkqlMetadata() {
		MathPkqlMetadata metadata = new MathPkqlMetadata();
		metadata.setPkqlStr((String) myStore.get(PKQLEnum.MATH_FUN));
		metadata.setColumnsOperatedOn((Vector<String>) myStore.get(PKQLEnum.COL_DEF));
		metadata.setGroupByColumns((List<String>) myStore.get(PKQLEnum.COL_CSV));
		metadata.setProcedureName(mathRoutine);
		metadata.setAdditionalInfo(myStore.get("ADDITIONAL_INFO"));
		return metadata;
	}
	
	protected void setMathRoutine(String mathRoutine) {
		this.mathRoutine = mathRoutine;
	}
}
