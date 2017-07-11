package prerna.sablecc2.reactor.qs;

import prerna.query.interpreters.QueryStruct2;

public class DatabaseReactor extends QueryStructReactor {

	@Override
	QueryStruct2 createQueryStruct() {
		//get the selectors
		String engineName = (String)curRow.get(0);
		qs.setEngineName(engineName);
		return qs;
	}
}
