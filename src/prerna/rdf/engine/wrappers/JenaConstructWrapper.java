package prerna.rdf.engine.wrappers;

import prerna.rdf.engine.api.IConstructStatement;
import prerna.rdf.engine.api.IConstructWrapper;
import prerna.util.Utility;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.StmtIterator;

public class JenaConstructWrapper extends AbstractWrapper implements
		IConstructWrapper {
	
	transient Model model = null;
	transient StmtIterator si = null;
	

	@Override
	public IConstructStatement next() {
		IConstructStatement thisSt = new ConstructStatement();

		com.hp.hpl.jena.rdf.model.Statement stmt = si.next();
		logger.debug("Adding a JENA statement ");
		Resource sub = stmt.getSubject();
		Property pred = stmt.getPredicate();
		RDFNode node = stmt.getObject();
		if(node.isAnon())
			thisSt.setPredicate(Utility.getNextID());
		else 	
			thisSt.setPredicate(stmt.getPredicate() + "");

		if(sub.isAnon())
			thisSt.setSubject(Utility.getNextID());
		else
			thisSt.setSubject(stmt.getSubject()+"");
		
		if(node.isAnon())
			thisSt.setObject(Utility.getNextID());
		else
			thisSt.setObject(stmt.getObject());
		
		return thisSt;
	}

	@Override
	public void execute() {
		model = (Model)engine.execGraphQuery(query);
		si = model.listStatements();
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return si.hasNext();
	}

}
