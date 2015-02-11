package prerna.rdf.engine.wrappers;

import org.openrdf.model.Statement;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;

import prerna.rdf.engine.api.IConstructStatement;
import prerna.rdf.engine.api.IConstructWrapper;
import prerna.rdf.engine.impl.SesameJenaConstructStatement;

public class SesameConstructWrapper extends AbstractWrapper implements
		IConstructWrapper {

	public transient GraphQueryResult gqr = null;
	
	@Override
	public IConstructStatement next() {
		IConstructStatement thisSt = new ConstructStatement();
		try {
			logger.debug("Adding a sesame statement ");
			Statement stmt = gqr.next();
			thisSt.setSubject(stmt.getSubject()+"");
			thisSt.setObject(stmt.getObject());
			thisSt.setPredicate(stmt.getPredicate() + "");
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return thisSt;
	}

	@Override
	public void execute() {
		gqr = (GraphQueryResult)engine.execGraphQuery(this.query);
	}

	@Override
	public boolean hasNext() {
		boolean retBool = false;
		
		try {
			retBool = gqr.hasNext();
			if(!retBool)
				gqr.close();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return retBool;
	}
}
