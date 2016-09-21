package prerna.sablecc;

import java.util.Iterator;

public class DatabaseConceptsReactor extends AbstractReactor {

	public DatabaseConceptsReactor() {
		String[] thisReacts = { PKQLEnum.WORD_OR_NUM};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.DATABASE_CONCEPTS;
	}
	
	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		return null;
	}

}
