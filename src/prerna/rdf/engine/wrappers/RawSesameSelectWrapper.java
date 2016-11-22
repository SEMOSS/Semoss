package prerna.rdf.engine.wrappers;

import java.util.List;

import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.HeadersDataRow;
import prerna.util.Utility;

public class RawSesameSelectWrapper extends AbstractWrapper implements IRawSelectWrapper {

	private TupleQueryResult tqr = null;
	private int numColumns = 0;
	
	@Override
	public void execute() {
		tqr = (TupleQueryResult) engine.execQuery(query);
		// set the variables for future use
		setVariables();
	}

	@Override
	public boolean hasNext() {
		boolean retBool = false;
		try {
			retBool = tqr.hasNext();
			if (!retBool) {
				tqr.close();
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
				Object val = bindSet.getValue(var[colIndex]);
				// raw value is the straight return from the binding set
				rawRow[colIndex] = val;
				// get the real value in the clean row
				cleanRow[colIndex] = getRealValue(val);
			}
			
			return new HeadersDataRow(displayVar, cleanRow, rawRow);
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		return null;
	}

	
	private void setVariables() {
		try {
			// this makes the assumption that the query is constructed
			// using the logic within the SPARQL Query Builder
			
			// get the vars from the tuple result 
			List<String> names = tqr.getBindingNames();
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
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
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
	
	private Object getRealValue(Object val){
		try
		{
			if(val != null && val instanceof Literal) {
				// use datatype if present to determine the type
				if( ((Literal) val).getDatatype() != null) {
					// if string, return string
					if(QueryEvaluationUtil.isStringLiteral((Value) val)){
						return ((Literal)val).getLabel();
					}
					// if date as string
					else if((val.toString()).contains("http://www.w3.org/2001/XMLSchema#dateTime")){
						return (val.toString()).substring((val.toString()).indexOf("\"")+1, (val.toString()).lastIndexOf("\""));
					}
					else{
						logger.debug("This is a literal impl >>>>>> "  + ((Literal)val).doubleValue());
						return new Double(((Literal)val).doubleValue());
					}
				} else {
					// update, if no data type present, just send back the label
					
					// no datatype present need to try and see based on casting
//					try {
//						XMLGregorianCalendar calendar = ((Literal)val).calendarValue();
//						return calendar.toGregorianCalendar().getTime(); // return date object
//					} catch(IllegalArgumentException ex) {
//						// do nothing
//					}
//
//					try {
//						double dVal = ((Literal)val).doubleValue();
//						return dVal;
//					} catch(NumberFormatException ex) {
//						// do nothing
//					}
					
					return ((Literal)val).getLabel();
				}
			} else if(val != null && val instanceof com.hp.hpl.jena.rdf.model.Literal) {
				logger.debug("Class is " + val.getClass());
				return new Double(((Literal)val).doubleValue());
			}
			
			if(val!=null){
				String value = val+"";
				return Utility.getInstanceName(value);
			}
		} catch(RuntimeException ex) {
			logger.debug(ex);
		}
		return "";
	}

	
}