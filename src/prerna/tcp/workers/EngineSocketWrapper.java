package prerna.tcp.workers;

import java.util.Vector;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.tcp.PayloadStruct;
import prerna.tcp.SocketServerHandler;

public class EngineSocketWrapper extends AbstractDatabaseEngine {

	// base class for doing everything over the socket
	SocketServerHandler ssh = null;
	String engineId = null;
	
	public EngineSocketWrapper(String engineId, SocketServerHandler ssh)
	{
		this.engineId = engineId;
		this.ssh = ssh;
	}
	
	@Override
	public Object execQuery(String query) throws Exception {

		// TODO Auto-generated method stub
		String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.ENGINE;
		ps.methodName = methodName;
		ps.payload = new Object[] {query};
		ps.payloadClasses = new Class[] {String.class};
		ps.hasReturn = false;
		ps.objId = engineId;
		ps.response = false;
	
		PayloadStruct retStruct = ssh.writeResponse(ps);
		
		if(retStruct.ex != null)
			throw new RuntimeException(retStruct.ex);
		
		return retStruct.payload[0];
	}

	@Override
	public void insertData(String query) throws Exception 
	{
		String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.ENGINE;
		ps.methodName = methodName;
		ps.hasReturn = false;
		ps.objId = engineId;
		ps.response = false;
	
		PayloadStruct retStruct = ssh.writeResponse(ps);
		
		if(retStruct.ex != null)
			throw new RuntimeException(retStruct.ex);

	}

	@Override
	public void removeData(String query) throws Exception {
		String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.ENGINE;
		ps.methodName = methodName;
		ps.hasReturn = false;
		ps.objId = engineId;
		ps.response = false;
	
		PayloadStruct retStruct = ssh.writeResponse(ps);
		
		if(retStruct.ex != null)
			throw new RuntimeException(retStruct.ex);
	}

	@Override
	public void commit() {
		String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.ENGINE;
		ps.methodName = methodName;
		ps.hasReturn = false;
		ps.objId = engineId;
		ps.response = false;
	
		PayloadStruct retStruct = ssh.writeResponse(ps);
		
		if(retStruct.ex != null)
			throw new RuntimeException(retStruct.ex);
		
	}

	@Override
	public DATABASE_TYPE getDatabaseType() {
		// TODO Auto-generated method stub
		String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.ENGINE;
		ps.methodName = methodName;
		ps.hasReturn = false;
		ps.objId = engineId;
		ps.response = false;
	
		PayloadStruct retStruct = ssh.writeResponse(ps);
		
		if(retStruct.ex != null)
			throw new RuntimeException(retStruct.ex);
		
		return (IDatabaseEngine.DATABASE_TYPE)retStruct.payload[0];
	}

	@Override
	public Vector<Object> getEntityOfType(String type) {
		String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.ENGINE;
		ps.methodName = methodName;
		ps.hasReturn = false;
		ps.objId = engineId;
		ps.response = false;
	
		PayloadStruct retStruct = ssh.writeResponse(ps);
		
		if(retStruct.ex != null)
			throw new RuntimeException(retStruct.ex);
		
		return (Vector<Object>)retStruct.payload[0];
	}
	
	@Override
	public boolean holdsFileLocks() {
		String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.ENGINE;
		ps.methodName = methodName;
		ps.hasReturn = false;
		ps.objId = engineId;
		ps.response = false;
	
		PayloadStruct retStruct = ssh.writeResponse(ps);
		
		if(retStruct.ex != null)
			throw new RuntimeException(retStruct.ex);
		
		return (boolean)retStruct.payload[0];
	}

}
