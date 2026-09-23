/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.tcp.workers;

import java.util.Vector;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.tcp.PayloadStruct;
import prerna.tcp.SocketServerHandler;

public class EngineSocketWrapper extends AbstractDatabaseEngine {

	// base class for doing everything over the socket
	SocketServerHandler ssh = null;

	public EngineSocketWrapper(String engineId, SocketServerHandler ssh) {
		this.engineId = engineId;
		this.ssh = ssh;
	}

	@Override
	public Object execQuery(String query) throws Exception {
		String methodName = new Object() {
		}.getClass().getEnclosingMethod().getName();
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.ENGINE;
		ps.methodName = methodName;
		ps.payload = new Object[] { query };
		ps.payloadClasses = new Class[] { String.class };
		ps.hasReturn = false;
		ps.objId = engineId;
		ps.response = false;

		PayloadStruct retStruct = ssh.writeResponse(ps);

		if (retStruct.ex != null) {
			throw new RuntimeException(retStruct.ex);
		}

		return retStruct.payload[0];
	}

	@Override
	public void insertData(String query) throws Exception {
		String methodName = new Object() {
		}.getClass().getEnclosingMethod().getName();
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.ENGINE;
		ps.methodName = methodName;
		ps.hasReturn = false;
		ps.objId = engineId;
		ps.response = false;

		PayloadStruct retStruct = ssh.writeResponse(ps);

		if (retStruct.ex != null) {
			throw new RuntimeException(retStruct.ex);
		}
	}

	@Override
	public void removeData(String query) throws Exception {
		String methodName = new Object() {
		}.getClass().getEnclosingMethod().getName();
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.ENGINE;
		ps.methodName = methodName;
		ps.hasReturn = false;
		ps.objId = engineId;
		ps.response = false;

		PayloadStruct retStruct = ssh.writeResponse(ps);
		if (retStruct.ex != null) {
			throw new RuntimeException(retStruct.ex);
		}
	}

	@Override
	public void commit() {
		String methodName = new Object() {
		}.getClass().getEnclosingMethod().getName();
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.ENGINE;
		ps.methodName = methodName;
		ps.hasReturn = false;
		ps.objId = engineId;
		ps.response = false;

		PayloadStruct retStruct = ssh.writeResponse(ps);
		if (retStruct.ex != null) {
			throw new RuntimeException(retStruct.ex);
		}
	}

	@Override
	public DATABASE_TYPE getDatabaseType() {
		String methodName = new Object() {
		}.getClass().getEnclosingMethod().getName();
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.ENGINE;
		ps.methodName = methodName;
		ps.hasReturn = false;
		ps.objId = engineId;
		ps.response = false;

		PayloadStruct retStruct = ssh.writeResponse(ps);
		if (retStruct.ex != null) {
			throw new RuntimeException(retStruct.ex);
		}
		return (IDatabaseEngine.DATABASE_TYPE) retStruct.payload[0];
	}

	@Override
	public Vector<Object> getEntityOfType(String type) {
		String methodName = new Object() {
		}.getClass().getEnclosingMethod().getName();
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.ENGINE;
		ps.methodName = methodName;
		ps.hasReturn = false;
		ps.objId = engineId;
		ps.response = false;

		PayloadStruct retStruct = ssh.writeResponse(ps);
		if (retStruct.ex != null) {
			throw new RuntimeException(retStruct.ex);
		}
		return (Vector<Object>) retStruct.payload[0];
	}

	@Override
	public boolean holdsFileLocks() {
		String methodName = new Object() {
		}.getClass().getEnclosingMethod().getName();
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.ENGINE;
		ps.methodName = methodName;
		ps.hasReturn = false;
		ps.objId = engineId;
		ps.response = false;

		PayloadStruct retStruct = ssh.writeResponse(ps);
		if (retStruct.ex != null) {
			throw new RuntimeException(retStruct.ex);
		}
		return (boolean) retStruct.payload[0];
	}

}
