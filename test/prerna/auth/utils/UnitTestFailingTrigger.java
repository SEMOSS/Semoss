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
package prerna.auth.utils;

import java.sql.Connection;
import java.sql.SQLException;

import org.h2.api.Trigger;

import prerna.engine.api.IRDBMSEngine;

/**
 * An H2 trigger that refuses every row it fires for. A test installs it on the
 * statement of a transaction that should fail, and checks that the statements
 * before it are rolled back.
 */
public class UnitTestFailingTrigger implements Trigger {

	/** The message of the failure each row gets */
	static final String MESSAGE = "Refused by a test trigger";

	/**
	 * Makes every row of one kind of statement on a table fail, until the trigger
	 * is dropped.
	 *
	 * @param securityDb the security database
	 * @param name       the trigger's name, to drop it with
	 * @param event      INSERT, UPDATE or DELETE
	 * @param table      the table
	 * @throws Exception when the trigger cannot be made
	 */
	static void install(IRDBMSEngine securityDb, String name, String event, String table) throws Exception {
		securityDb.insertData("CREATE TRIGGER " + name + " BEFORE " + event + " ON " + table + " FOR EACH ROW CALL \""
				+ UnitTestFailingTrigger.class.getName() + "\"");
	}

	/**
	 * Drops a trigger {@link #install} made, when it is there.
	 *
	 * @param securityDb the security database
	 * @param name       the trigger's name
	 * @throws Exception when the trigger cannot be dropped
	 */
	static void drop(IRDBMSEngine securityDb, String name) throws Exception {
		securityDb.insertData("DROP TRIGGER IF EXISTS " + name);
	}

	@Override
	public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before,
			int type) {
		// nothing to read
	}

	@Override
	public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
		throw new SQLException(MESSAGE);
	}

	@Override
	public void close() {
		// nothing to release
	}

	@Override
	public void remove() {
		// nothing to release
	}
}
