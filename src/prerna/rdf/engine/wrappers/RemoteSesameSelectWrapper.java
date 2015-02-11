package prerna.rdf.engine.wrappers;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Hashtable;

import org.openrdf.query.BindingSet;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Utility;

public class RemoteSesameSelectWrapper extends SesameSelectWrapper implements	ISelectWrapper {

	transient SesameSelectWrapper remoteWrapperProxy = null;
	transient ISelectStatement retSt = null;
	transient boolean retBool = false;
	transient ObjectInputStream ris = null;


	@Override
	public void execute() {
		System.out.println("Trying to get the wrapper remotely now");
		remoteWrapperProxy = (SesameSelectWrapper)engine.execSelectQuery(query);
		var = remoteWrapperProxy.getVariables();
		System.out.println("Output variables is " + remoteWrapperProxy.getVariables());
	}

	@Override
	public boolean hasNext() {
		
		if(retSt != null) // this means they have not picked it up yet
			return true;
		//retSt = new SelectStatement();

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
				BindingSet bs = (BindingSet)myObject;
				//System.out.println("Proceeded to first");
				retSt = getSelectFromBinding(bs);			
				retBool = true;
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retBool;
	}

	@Override
	public ISelectStatement next() {
		// TODO Auto-generated method stub
		ISelectStatement thisSt = retSt;
		retSt = null;
		return thisSt;
	}

	@Override
	public String[] getVariables() {
		// TODO Auto-generated method stub
		return var;
	}
}
