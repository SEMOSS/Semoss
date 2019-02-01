package prerna.rdf.engine.wrappers;

import java.util.List;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.HeadersDataRow;
import prerna.util.Utility;

public class RawJenaSelectWrapper  extends AbstractWrapper implements IRawSelectWrapper {

	private static final Logger LOGGER = LogManager.getLogger(RawJenaSelectWrapper.class.getName());
	private ResultSet rs = null;

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
		for(int colIndex = 0;colIndex < numColumns; colIndex++) {
			RDFNode node = row.get(rawHeaders[colIndex]);
			// raw value is the straight return from the binding set
			rawRow[colIndex] = node.toString();
			// get the real value of the node
			cleanRow[colIndex] = getRealValue(node);
		}

		return new HeadersDataRow(headers, cleanRow, rawRow);
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
		rawHeaders = names.toArray(new String[]{});

		headers = new String[numColumns];
		for(int colIndex = 0; colIndex < numColumns; colIndex++){
			// for the display, if we encounter a "__", we want to 
			// split and get the second part of the string
			// that is the display for the column
			String columnLabel = names.get(colIndex);
			if(columnLabel.contains("__")){
				String[] splitColAndTable = columnLabel.split("__");
				columnLabel = splitColAndTable[1];
			}
			headers[colIndex] = columnLabel;
		}

	}

	@Override
	public String[] getHeaders() {
		return headers;
	}

	private Object getRealValue(RDFNode node){
		if(node.isAnon()) {
			LOGGER.debug("Ok.. an anon node");
			return Utility.getNextID();
		} else {
			LOGGER.debug("Raw data JENA For Column ");
			return Utility.getInstanceName(node + "");
		}
	}
	
	@Override
	public SemossDataType[] getTypes() {
		if(this.types == null) {
			try {
				SPARQLParser parser = new SPARQLParser();
				ParsedQuery parsedQuery = parser.parseQuery(query, null);

				CustomSparqlAggregationParser aggregationVisitor = new CustomSparqlAggregationParser();
				parsedQuery.getTupleExpr().visit(aggregationVisitor);
				Set<String> aggregationValues = aggregationVisitor.getValue();
				
				this.types = new SemossDataType[this.numColumns];
				for(int i = 0; i < this.numColumns; i++) {
					if(aggregationValues.contains(this.rawHeaders[i])) {
						this.types[i] = SemossDataType.DOUBLE;
					} else {
						this.types[i] = SemossDataType.STRING;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				this.types = new SemossDataType[this.numColumns];
				for(int i = 0; i < this.numColumns; i++) {
					this.types[i] = SemossDataType.STRING;
				}
			}
		}
		return this.types;
	}

	@Override
	public void cleanUp() {
		// TODO Auto-generated method stub
		// there is no close on this rs object...
	}
	
	@Override
	public long getNumRows() {
		if(this.numRows == 0) {
			String query = "select count(*) where { " + this.query + "}";
			ResultSet rs = (ResultSet) engine.execQuery(query);
			if(rs.hasNext()) {
				QuerySolution row = rs.next();
				RDFNode node = row.get("count");
				Object cleanValue = getRealValue(node);
				if(cleanValue instanceof Number) {
					this.numRows = ((Number) cleanValue).longValue();
				}
			}
		}
		return this.numRows;
	}
	
	@Override
	public long getNumRecords() {
		return getNumRows() * this.numColumns;
	}
	
	@Override
	public void reset() {
		cleanUp();
		execute();
	}

}
