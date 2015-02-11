package prerna.rdf.engine.wrappers;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import prerna.rdf.engine.api.IConstructStatement;
import prerna.rdf.engine.api.IConstructWrapper;

public class SesameSelectCheater extends AbstractWrapper implements
		IConstructWrapper {

	transient TupleQueryResult tqr = null;
	transient int count = 0;
	transient String [] var = null;
	transient BindingSet bs = null;
	transient int triples;
	transient int tqrCount=0;
	String queryVar[];
	
	@Override
	public IConstructStatement next() {
		IConstructStatement thisSt = new ConstructStatement();
		
		try {
			if(count==0)
			{
				bs = tqr.next();
			}
			logger.debug("Adding a sesame statement ");
			
			// there should only be three values
			Object sub=null;
			Object pred = null;
			Object obj = null;
			while (sub==null || pred==null || obj==null)
			{
				if (count==triples)
				{
					count=0;
					bs = tqr.next();
					tqrCount++;
					//logger.info(tqrCount);
				}
				sub = bs.getValue(queryVar[count*3].substring(1));
				pred = bs.getValue(queryVar[count*3+1].substring(1));
				obj = bs.getValue(queryVar[count*3+2].substring(1));
				count++;
			}
			thisSt.setSubject(sub+"");
			thisSt.setPredicate(pred+"");
			thisSt.setObject(obj);
			if (count==triples)
			{
				count=0;
			}
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return thisSt;
	}
	
	protected void processSelectVar()
	{
		if(query.contains("DISTINCT"))
		{
			Pattern pattern = Pattern.compile("SELECT DISTINCT(.*?)WHERE");
		    Matcher matcher = pattern.matcher(query);
		    String varString = null;
		    while (matcher.find()) 
		    {
		    	varString = matcher.group(1);
		    }
		    varString = varString.trim();
		    queryVar = varString.split(" ");
		    int num = queryVar.length+1;
		    triples = (int) Math.floor(num/3);
		}
		else
		{
			Pattern pattern = Pattern.compile("SELECT (.*?)WHERE");
		    Matcher matcher = pattern.matcher(query);
		    String varString = null;
		    while (matcher.find()) {
		        varString = matcher.group(1);
		    }
		    varString = varString.trim();
		    queryVar = varString.split(" ");
		    int num = queryVar.length+1;
		    triples = (int) Math.floor(num/3);
		}
	}

	protected String[] getVariables()
	{
		try {
			var = new String[tqr.getBindingNames().size()];
			List <String> names = tqr.getBindingNames();
			for(int colIndex = 0;colIndex < names.size();var[colIndex] = names.get(colIndex), colIndex++);
			return var;
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void execute() {
		tqr = (TupleQueryResult) engine.execSelectQuery(query);
		getVariables();
		
		processSelectVar();
		count=0;
	}

	@Override
	public boolean hasNext() {
		boolean retBool = false;
		try {
			retBool = tqr.hasNext();
			if(!retBool)
				tqr.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retBool;
	}

}
