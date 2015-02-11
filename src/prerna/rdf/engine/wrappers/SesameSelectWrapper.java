package prerna.rdf.engine.wrappers;

import java.util.List;

import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;

import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.util.Utility;

public class SesameSelectWrapper extends AbstractWrapper implements
		ISelectWrapper {

	transient TupleQueryResult tqr = null;

	@Override
	public void execute() {
		// TODO Auto-generated method stub
		tqr = (TupleQueryResult) engine.execSelectQuery(query);
	}

	@Override
	public boolean hasNext() {
		boolean retBool = false;
		try {
			retBool = tqr.hasNext();
			if (!retBool)
				tqr.close();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retBool;
	}

	@Override
	public ISelectStatement next() {
		// TODO Auto-generated method stub
		ISelectStatement sjss = null;
		// thisSt = new SesameJenaSelectStatement();
		try {
			logger.debug("Adding a sesame statement ");
			BindingSet bs = tqr.next();
			sjss = getSelectFromBinding(bs);
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sjss;
	}

	protected ISelectStatement getSelectFromBinding(BindingSet bs)
	{
		ISelectStatement sjss = new SelectStatement();
		for(int colIndex = 0;colIndex < var.length;colIndex++)
		{
			Object val = bs.getValue(var[colIndex]);
			Double weightVal = null;
			String dateStr=null;
			String stringVal = null;
			try
			{
				if(val != null && val instanceof Literal)
				{
					if(QueryEvaluationUtil.isStringLiteral((Value) val)){
						stringVal = ((Literal)val).getLabel();
					}
					else if((val.toString()).contains("http://www.w3.org/2001/XMLSchema#dateTime")){
						dateStr = (val.toString()).substring((val.toString()).indexOf("\"")+1, (val.toString()).lastIndexOf("\""));
					}
					else{
						logger.debug("This is a literal impl >>>>>> "  + ((Literal)val).doubleValue());
						weightVal = new Double(((Literal)val).doubleValue());
					}
				}else if(val != null && val instanceof com.hp.hpl.jena.rdf.model.Literal)
				{
					logger.debug("Class is " + val.getClass());
					weightVal = new Double(((Literal)val).doubleValue());
				}
			}catch(RuntimeException ex)
			{
				logger.debug(ex);
			}
			String value = bs.getValue(var[colIndex])+"";
			String instanceName = Utility.getInstanceName(value);
			if(weightVal == null && dateStr==null && stringVal==null && val != null)
				sjss.setVar(var[colIndex], instanceName);
			else if (weightVal != null)
				sjss.setVar(var[colIndex], weightVal);
			else if (dateStr != null)
				sjss.setVar(var[colIndex], dateStr);
			else if (stringVal != null)
				sjss.setVar(var[colIndex], stringVal);
			else if(val == null) {
				sjss.setVar(var[colIndex], "");
				continue;
			}
			sjss.setRawVar(var[colIndex], val);
			logger.debug("Binding Name " + var[colIndex]);
			logger.debug("Binding Value " + value);
		}
		return sjss;
	}

	@Override
	public String[] getVariables() {
		try {
			var = new String[tqr.getBindingNames().size()];
			List<String> names = tqr.getBindingNames();
			for (int colIndex = 0; colIndex < names.size(); var[colIndex] = names
					.get(colIndex), colIndex++)
				;
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return var;
	}
}
