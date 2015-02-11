package prerna.rdf.engine.wrappers;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Hashtable;

import org.openrdf.model.Statement;

import prerna.rdf.engine.api.IConstructStatement;
import prerna.rdf.engine.api.IConstructWrapper;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaConstructStatement;
import prerna.rdf.engine.impl.SesameJenaConstructWrapper;
import prerna.util.Utility;

public class RemoteSesameConstructWrapper extends AbstractWrapper implements IConstructWrapper {

	transient SesameConstructWrapper remoteWrapperProxy = null;
	transient IConstructStatement retSt = null;
	transient ObjectInputStream ris = null;


	@Override
	public void execute() {
		remoteWrapperProxy = (SesameConstructWrapper)engine.execGraphQuery(query);
	}

	@Override
	public boolean hasNext() {
		boolean retBool = false;
		
		if(retSt != null) // they have not picked it up yet
			return true;
		retSt = new ConstructStatement();
		// I need to pull from remote
		// this is just so stupid to call its own
		try {
		if(ris == null)
		{
			Hashtable params = new Hashtable<String,String>();
			params.put("id", remoteWrapperProxy.getRemoteID());
			ris = new ObjectInputStream(Utility.getStream(remoteWrapperProxy.getRemoteAPI() + "/next", params));
		}					
			Object myObject = ris.readObject();
			
			if(!myObject.toString().equalsIgnoreCase("null"))
			{
				Statement stmt = (Statement)myObject;
				retSt.setSubject(stmt.getSubject()+"");
				retSt.setObject(stmt.getObject());
				retSt.setPredicate(stmt.getPredicate() + "");
				//System.out.println("Abile to get the object appropriately here " + retSt.getSubject());
				retBool = true;
			}
			else
			{
				try{
					if(ris!=null) {
						ris.close();
					}
				} catch(IOException e) {
					e.printStackTrace();
				}
			}

		} catch (RuntimeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			retSt = null;
			retBool = false;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			retSt = null;
			retBool = false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			retSt = null;
			retBool = false;
		}
		return retBool;
	}

	@Override
	public IConstructStatement next() {
		// TODO Auto-generated method stub
		IConstructStatement thisSt = retSt;
		retSt = null;
		return thisSt;
	}

}
