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
package prerna.engine.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The result of running SQL statements in one transaction with
 * {@link IRDBMSEngine#executeInTransaction(List, int, int)}: what the
 * transaction left in the database and what happened to each statement.
 */
public final class SqlTransactionResult {

	/** What the transaction left in the database */
	public enum Outcome {
		/** Every statement ran and the commit succeeded */
		COMMITTED,
		/** No change was kept */
		ROLLED_BACK,
		/**
		 * The commit was sent and no answer came back, so the database has to be
		 * checked to know whether the changes were kept
		 */
		UNKNOWN
	}

	/** What happened to one statement */
	public enum Status {
		/** The statement ran and its change was committed, or the outcome is unknown */
		SUCCESS,
		/** The statement failed, or the time limit was reached before it ran */
		ERROR,
		/** The statement ran and its change was rolled back */
		ROLLED_BACK,
		/** The statement did not run because an earlier one failed */
		SKIPPED
	}

	/**
	 * What happened to one statement.
	 */
	public static final class StatementResult {

		private final int statement;
		private Status status = Status.SKIPPED;
		private boolean returnedRows;
		// transient, so the engine call's audit log entry, which serializes the result
		// with GSON, keeps no table data
		private transient List<String> headers;
		private transient List<Object[]> rows;
		private boolean hasMoreRows;
		private Integer updateCount;
		private long timeToRun;
		private String sqlState;
		private String message;

		StatementResult(int statement) {
			this.statement = statement;
		}

		/**
		 * The statement's position in the script.
		 *
		 * @return the 1-based position
		 */
		public int getStatement() {
			return this.statement;
		}

		/**
		 * What happened to the statement.
		 *
		 * @return the status
		 */
		public Status getStatus() {
			return this.status;
		}

		/**
		 * Whether the statement returned rows, whether or not any were kept.
		 *
		 * @return true for a statement that returned rows
		 */
		public boolean isReturnedRows() {
			return this.returnedRows;
		}

		/**
		 * The column names of the rows kept from the statement.
		 *
		 * @return the names, or null when no rows were kept
		 */
		public List<String> getHeaders() {
			return this.headers;
		}

		/**
		 * The rows kept from the statement, up to the most asked for, with each value
		 * converted as a query outside a transaction converts it. They are left out of
		 * the engine call's audit log entry.
		 *
		 * @return the rows, or null when no rows were kept
		 */
		public List<Object[]> getRows() {
			return this.rows;
		}

		/**
		 * Whether the statement returned more rows than were kept.
		 *
		 * @return true when rows were left out
		 */
		public boolean hasMoreRows() {
			return this.hasMoreRows;
		}

		/**
		 * The number of rows the statement changed, as the driver reported it.
		 *
		 * @return the count, or null when the driver did not report one
		 */
		public Integer getUpdateCount() {
			return this.updateCount;
		}

		/**
		 * How long the statement ran.
		 *
		 * @return milliseconds
		 */
		public long getTimeToRun() {
			return this.timeToRun;
		}

		/**
		 * The SQLState of the statement's error.
		 *
		 * @return the SQLState, or null when the statement did not fail or the driver
		 *         gave none
		 */
		public String getSqlState() {
			return this.sqlState;
		}

		/**
		 * Why the statement failed.
		 *
		 * @return the error message, or null when the statement did not fail
		 */
		public String getMessage() {
			return this.message;
		}
	}

	private Outcome outcome = Outcome.ROLLED_BACK;
	private final List<StatementResult> statements;
	private Integer failedStatement;
	private String message;

	/**
	 * A result for a script whose statements have not run yet.
	 *
	 * @param statementCount the number of statements in the script
	 */
	public SqlTransactionResult(int statementCount) {
		List<StatementResult> results = new ArrayList<>(statementCount);
		for (int index = 0; index < statementCount; index++) {
			results.add(new StatementResult(index + 1));
		}
		this.statements = Collections.unmodifiableList(results);
	}

	/**
	 * Records a statement that ran without error.
	 *
	 * @param index        the 0-based position of the statement
	 * @param returnedRows whether it returned rows
	 * @param updateCount  the rows it changed, or null when the driver did not say
	 * @param timeToRun    how long it ran, in milliseconds
	 */
	public void succeeded(int index, boolean returnedRows, Integer updateCount, long timeToRun) {
		StatementResult result = this.statements.get(index);
		result.status = Status.SUCCESS;
		result.returnedRows = returnedRows;
		result.updateCount = updateCount;
		result.timeToRun = timeToRun;
	}

	/**
	 * Records the rows kept from a statement that returned rows.
	 *
	 * @param index       the 0-based position of the statement
	 * @param headers     the column names
	 * @param rows        the rows kept, up to the most asked for
	 * @param hasMoreRows whether the statement returned more rows than were kept
	 */
	public void keptRows(int index, List<String> headers, List<Object[]> rows, boolean hasMoreRows) {
		StatementResult result = this.statements.get(index);
		result.headers = headers;
		result.rows = rows;
		result.hasMoreRows = hasMoreRows;
	}

	/**
	 * Records the statement that stopped the script. The statements after it stay
	 * skipped.
	 *
	 * @param index     the 0-based position of the statement
	 * @param sqlState  the SQLState of the error, or null
	 * @param message   why it failed
	 * @param timeToRun how long it ran before failing, in milliseconds
	 */
	public void failed(int index, String sqlState, String message, long timeToRun) {
		StatementResult result = this.statements.get(index);
		result.status = Status.ERROR;
		result.sqlState = sqlState;
		result.message = message;
		result.timeToRun = timeToRun;
		this.failedStatement = index + 1;
		this.message = "Statement " + (index + 1) + " of " + this.statements.size() + " failed: " + message;
	}

	/**
	 * Records that no change was kept. Every statement that ran becomes rolled
	 * back.
	 *
	 * @param message why, when no statement failed (a rejected commit), or null to
	 *                keep the failed statement's message
	 */
	public void rolledBack(String message) {
		this.outcome = Outcome.ROLLED_BACK;
		for (StatementResult result : this.statements) {
			if (result.status == Status.SUCCESS) {
				result.status = Status.ROLLED_BACK;
			}
		}
		if (message != null) {
			this.message = message;
		}
	}

	/**
	 * Records that the commit succeeded.
	 */
	public void committed() {
		this.outcome = Outcome.COMMITTED;
		this.message = null;
	}

	/**
	 * Records that whether the changes were kept is unknown: the commit was sent
	 * and no answer came back, or a rollback failed on a connection that may commit
	 * when it closes.
	 *
	 * @param message what happened, with what the driver reported
	 */
	public void unknown(String message) {
		this.outcome = Outcome.UNKNOWN;
		this.message = message;
	}

	/**
	 * What the transaction left in the database.
	 *
	 * @return the outcome
	 */
	public Outcome getOutcome() {
		return this.outcome;
	}

	/**
	 * What happened to each statement, in script order.
	 *
	 * @return one result per statement
	 */
	public List<StatementResult> getStatements() {
		return this.statements;
	}

	/**
	 * The statement that stopped the script.
	 *
	 * @return its 1-based position, or null when no statement failed
	 */
	public Integer getFailedStatement() {
		return this.failedStatement;
	}

	/**
	 * Why the transaction did not commit.
	 *
	 * @return the reason, or null after a commit
	 */
	public String getMessage() {
		return this.message;
	}

	/**
	 * The number of statements that ran without error, whether or not their changes
	 * were kept.
	 *
	 * @return the count
	 */
	public int getRanCount() {
		int ran = 0;
		for (StatementResult result : this.statements) {
			if (result.status == Status.SUCCESS || result.status == Status.ROLLED_BACK) {
				ran++;
			}
		}
		return ran;
	}
}
