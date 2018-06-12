package prerna.engine.api.iterator;

import java.util.List;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.rdf.engine.wrappers.CustomSparqlAggregationParser;
import prerna.util.Utility;

public class JenaDatasourceIterator extends AbstractDatasourceIterator {

	private static Logger LOGGER = LogManager.getLogger(SesameDatasourceIterator.class.getName());

	protected Model jenaModel;
	protected ResultSet rs = null;
	
	public JenaDatasourceIterator(Model jenaModel) {
		this.jenaModel = jenaModel;
	}
	
	@Override
	public void execute() {
		Query queryObject = QueryFactory.create(this.query); 
		QueryExecution qexec = QueryExecutionFactory.create(queryObject, jenaModel);
		this.rs = qexec.execSelect();
		List <String> names = rs.getResultVars();
		numColumns = names.size();

		// what should be in physical names?
		// we technically need the concept and prop name
		// this is already what we have via the names binding
		// when it is created through query builder
		this.rawHeaders = names.toArray(new String[]{});
		this.headers = new String[numColumns];
		for(int colIndex = 0; colIndex < numColumns; colIndex++){
			// for the display, if we encounter a "__", we want to 
			// split and get the second part of the string
			// that is the display for the column
			String columnLabel = names.get(colIndex);
			if(columnLabel.contains("__")){
				String[] splitColAndTable = columnLabel.split("__");
				columnLabel = splitColAndTable[1];
			}
			this.headers[colIndex] = columnLabel;
		}
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
			RDFNode node = row.get(this.headers[colIndex]);
			// raw value is the straight return from the binding set
			rawRow[colIndex] = node.toString();
			// get the real value of the node
			cleanRow[colIndex] = getRealValue(node);
		}

		return new HeadersDataRow(this.headers, this.rawHeaders, cleanRow, rawRow);
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
	public void cleanUp() {
		// TODO Auto-generated method stub
	}
	
	@Override
	public long getNumRecords() {
		String query = "select (count(*) * " + this.numColumns + " as ?count) where { " + this.query + "}";
		Query queryObject = QueryFactory.create(query); 
		QueryExecution qexec = QueryExecutionFactory.create(queryObject, jenaModel);
		ResultSet rs = qexec.execSelect();
		if(rs.hasNext()) {
			QuerySolution row = rs.next();
			RDFNode node = row.get("count");
			Object cleanValue = getRealValue(node);
			if(cleanValue instanceof Number) {
				return ((Number) cleanValue).longValue();
			}
		}
		return 0;
	}
	
	@Override
	public void reset() {
		cleanUp();
		execute();
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
					if(aggregationValues.contains(this.headers[i])) {
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
}
