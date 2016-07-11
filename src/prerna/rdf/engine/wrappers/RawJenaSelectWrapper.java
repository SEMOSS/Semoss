package prerna.rdf.engine.wrappers;

import java.util.List;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.HeadersDataRow;
import prerna.util.Utility;

public class RawJenaSelectWrapper  extends AbstractWrapper implements IRawSelectWrapper {

	private ResultSet rs = null;
	private int numColumns = 0;

	@Override
	public void execute() {
		rs = (ResultSet) engine.execQuery(query);
		// set the variables for future use
		setVariables();
	}

	@Override
	public boolean hasNext() {
		return rs.hasNext();
	}

	@Override
	public IHeadersDataRow next() {
		// need to store both the clean and raw values
		Object[] cleanRow = new Object[numColumns];
		Object[] rawRow = new Object[numColumns];

		QuerySolution row = rs.next();
		for(int colIndex = 0;colIndex < numColumns; colIndex++)
		{
			RDFNode node = row.get(var[colIndex]);
			// raw value is the straight return from the binding set
			rawRow[colIndex] = node.toString();
			// get the real value of the node
			cleanRow[colIndex] = getRealValue(node);
		}

		return new HeadersDataRow(displayVar, cleanRow, rawRow);
	}


	private void setVariables() {
		// this makes the assumption that the query is constructed
		// using the logic within the SPARQL Query Builder

		// get the vars from the tuple result 
		List <String> names = rs.getResultVars();
		numColumns = names.size();

		// what should be in physical names?
		// we technically need the concept and prop name
		// this is already what we have via the names binding
		// when it is created through query builder
		var = names.toArray(new String[]{});

		displayVar = new String[numColumns];
		for(int colIndex = 0; colIndex < numColumns; colIndex++){
			// for the display, if we encounter a "__", we want to 
			// split and get the second part of the string
			// that is the display for the column
			String columnLabel = names.get(colIndex);
			if(columnLabel.contains("__")){
				String[] splitColAndTable = columnLabel.split("__");
				columnLabel = splitColAndTable[1];
			}
			displayVar[colIndex] = columnLabel;
		}

	}

	@Override
	public String[] getDisplayVariables() {
		return displayVar;
	}

	@Override
	public String[] getPhysicalVariables() {
		return var;
	}

	private Object getRealValue(RDFNode node){
		if(node.isAnon()) {
			logger.debug("Ok.. an anon node");
			return Utility.getNextID();
		} else {
			logger.debug("Raw data JENA For Column ");
			return Utility.getInstanceName(node + "");
		}
	}

}
