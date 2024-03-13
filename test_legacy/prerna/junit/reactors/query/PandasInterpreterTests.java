package prerna.junit.reactors.query;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.junit.Test;

import prerna.ds.py.PandasFrame;
import prerna.query.interpreters.PandasInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;

public class PandasInterpreterTests {

	@Test
	public void testBasicComposeQuery() {
		PandasInterpreter pi = new PandasInterpreter();
		SelectQueryStruct sqs = new SelectQueryStruct();
		sqs.addSelector(new QueryColumnSelector("TEST__TESTID"));
		pi.setQueryStruct(sqs);
		PandasFrame pf = new PandasFrame();
		pf.setName("test12345");
		pi.setPandasFrame(pf);
		pi.setDataTypeMap(new HashMap<>());
		pi.setDataTableName("test12345", "test12345");
		String test = pi.composeQuery();
		assertEquals("test12345[['TESTID']].drop_duplicates().iloc[0:].to_dict('split')", test);
	}

}
