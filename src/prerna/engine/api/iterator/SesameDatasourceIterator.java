package prerna.engine.api.iterator;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;

import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.rdf.engine.wrappers.CustomSparqlAggregationParser;
import prerna.util.Utility;

public class SesameDatasourceIterator extends AbstractDatasourceIterator {

	private static Logger LOGGER = LogManager.getLogger(SesameDatasourceIterator.class.getName());
	private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.ssss'Z'");

	protected RepositoryConnection rc;
	protected TupleQueryResult tqr = null;
	
	public SesameDatasourceIterator(RepositoryConnection rc) {
		this.rc = rc;
	}
	
	@Override
	public void execute() {
		try {
			Query fullQuery = rc.prepareQuery(QueryLanguage.SPARQL, this.query);
			fullQuery.setIncludeInferred(true);
			this.tqr = ((TupleQuery) fullQuery).evaluate();
			
			List<String> names = tqr.getBindingNames();
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
		} catch (RepositoryException | MalformedQueryException | QueryEvaluationException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public boolean hasNext() {
		boolean retBool = false;
		try {
			retBool = this.tqr.hasNext();
			if (!retBool) {
				this.tqr.close();
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		return retBool;
	}
	
	@Override
	public IHeadersDataRow next() {
		try {
			// need to store both the clean and raw values
			Object[] cleanRow = new Object[numColumns];
			Object[] rawRow = new Object[numColumns];

			BindingSet bindSet = tqr.next();
			for(int colIndex = 0;colIndex < numColumns; colIndex++)
			{
				Object val = bindSet.getValue(this.headers[colIndex]);
				// raw value is the straight return from the binding set
				rawRow[colIndex] = val;
				// get the real value in the clean row
				cleanRow[colIndex] = getRealValue(val);
			}
			
			return new HeadersDataRow(this.headers, this.rawHeaders, cleanRow, rawRow);
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		return null;
	}
	
	private Object getRealValue(Object val){
		try
		{
			if(val != null && val instanceof Literal) {
				// use datatype if present to determine the type
				Literal lVal = (Literal) val;
				URI lValDataType = lVal.getDatatype();
				if(lValDataType != null) {
					// if string, return string
					if(QueryEvaluationUtil.isStringLiteral((Value) val)){
						return ((Literal)val).getLabel();
					}
					// if datetime
					else if(lValDataType.getLocalName().equalsIgnoreCase("dateTime")) {
						try {
							Date d = formatter.parse(lVal.calendarValue().toString());
							SemossDate date = new SemossDate(d, "yyyy-MM-dd hh:mm:ss");
							return date;
						} catch (ParseException e) {
							e.printStackTrace();
						}
						return null;
					} 
					// if date
					else if(lValDataType.getLocalName().equalsIgnoreCase("date")) {
						XMLGregorianCalendar gCalendar = lVal.calendarValue();
						SemossDate date = new SemossDate(gCalendar.toGregorianCalendar().getTime(), "yyyy-MM-dd");
						return date;
					} 
					// else double
					else{
						LOGGER.debug("This is a literal impl >>>>>> "  + ((Literal)val).doubleValue());
						return new Double(((Literal)val).doubleValue());
					}
				} else {
					// just return the label
					return ((Literal)val).getLabel();
				}
			} else if(val != null && val instanceof com.hp.hpl.jena.rdf.model.Literal) {
				LOGGER.debug("Class is " + val.getClass());
				return new Double(((Literal)val).doubleValue());
			}
			
			if(val!=null){
				String value = val+"";
				return Utility.getInstanceName(value);
			}
		} catch(RuntimeException ex) {
			LOGGER.debug(ex);
		}
		return "";
	}

	@Override
	public void cleanUp() {
		try {
			tqr.close();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public long getNumRecords() {
		String query = "select (count(*) * " + this.numColumns + " as ?count) where { " + this.query + "}";
		try {
			Query fullQuery = rc.prepareQuery(QueryLanguage.SPARQL, query);
			fullQuery.setIncludeInferred(true);
			TupleQueryResult tqr = ((TupleQuery) fullQuery).evaluate();
			
			if(tqr.hasNext()) {
				BindingSet bindSet = tqr.next();
				Object val = bindSet.getValue("count");
				Object cleanValue = getRealValue(val);
				if(cleanValue instanceof Number) {
					return ((Number) cleanValue).longValue();
				}
			}
		} catch (RepositoryException | MalformedQueryException | QueryEvaluationException e) {
			e.printStackTrace();
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
