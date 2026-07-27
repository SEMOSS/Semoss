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
package prerna.rdf.engine.wrappers;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.aggregate.AggregatorFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.RDFJenaTDBEngine;
import prerna.om.HeadersDataRow;
import prerna.om.ThreadStore;
import prerna.usertracking.UserQueryTrackingThread;
import prerna.util.Utility;

public class RawJenaTDBSelectWrapper extends AbstractWrapper implements IRawSelectWrapper {

	private static final Logger classLogger = LogManager.getLogger(RawJenaTDBSelectWrapper.class);
	private Dataset dataset = null;
	private ResultSet rs = null;

	@Override
	public void execute() throws Exception {
		Map<String, Object> map = (Map<String, Object>) engine.execQuery(query);
		dataset = (Dataset) map.get(RDFJenaTDBEngine.DATASET_OBJECT);
		rs = (ResultSet) map.get(RDFJenaTDBEngine.QUERY_RETURN);
		// set the variables for future use
		setVariables();
	}

	@Override
	public boolean hasNext() {
		return rs.hasNext();
	}

	@Override
	public IHeadersDataRow next() {
		// need to store both the clean and raw values
		Object[] cleanRow = new Object[numColumns];
		Object[] rawRow = new Object[numColumns];

		QuerySolution row = rs.next();
		for (int colIndex = 0; colIndex < numColumns; colIndex++) {
			RDFNode node = row.get(rawHeaders[colIndex]);
			// raw value is the straight return from the binding set
			if (node != null) {
				rawRow[colIndex] = node.toString();
				// get the real value of the node
				cleanRow[colIndex] = getRealValue(node);
			}
		}

		return new HeadersDataRow(headers, cleanRow, rawRow);
	}

	private void setVariables() {
		// this makes the assumption that the query is constructed
		// using the logic within the SPARQL Query Builder

		// get the vars from the tuple result
		List<String> names = rs.getResultVars();
		numColumns = names.size();

		// what should be in physical names?
		// we technically need the concept and prop name
		// this is already what we have via the names binding
		// when it is created through query builder
		rawHeaders = names.toArray(new String[names.size()]);

		headers = new String[numColumns];
		for (int colIndex = 0; colIndex < numColumns; colIndex++) {
			// for the display, if we encounter a "__", we want to
			// split and get the second part of the string
			// that is the display for the column
			String columnLabel = names.get(colIndex);
			if (columnLabel.contains("__")) {
				String[] splitColAndTable = columnLabel.split("__");
				columnLabel = splitColAndTable[1];
			}
			headers[colIndex] = columnLabel;
		}

	}

	@Override
	public String[] getHeaders() {
		return headers;
	}

	private Object getRealValue(RDFNode node) {
		if (node.isLiteral()) {
			return node.asLiteral().getValue();
		}
		return Utility.getInstanceName(node + "");
	}

	@Override
	public SemossDataType[] getTypes() {
		if (this.types == null) {
			try {
				SPARQLParser parser = new SPARQLParser();
				ParsedQuery parsedQuery = parser.parseQuery(query, null);

				CustomSparqlAggregationParser aggregationVisitor = new CustomSparqlAggregationParser();
				parsedQuery.getTupleExpr().visit(aggregationVisitor);
				Set<String> aggregationValues = aggregationVisitor.getValue();

				this.types = new SemossDataType[this.numColumns];
				for (int i = 0; i < this.numColumns; i++) {
					if (aggregationValues.contains(this.rawHeaders[i])) {
						this.types[i] = SemossDataType.DOUBLE;
					} else {
						this.types[i] = SemossDataType.STRING;
					}
				}
			} catch (Exception e) {
				classLogger.error(
						"Error parsing query to determine column data types; defaulting all columns to STRING", e);
				this.types = new SemossDataType[this.numColumns];
				for (int i = 0; i < this.numColumns; i++) {
					this.types[i] = SemossDataType.STRING;
				}
			}
		}
		return this.types;
	}

	@Override
	public void close() throws IOException {
		try {
			if (this.dataset != null) {
				this.dataset.end();
			}
		} catch (Exception e) {
			classLogger.error("Error closing the Jena TDB dataset", e);
		}
	}

	@Override
	public long getNumRows() {
		if (this.numRows == 0) {
			User user = ThreadStore.getUser();
			UserQueryTrackingThread queryT = new UserQueryTrackingThread(user, this.engine.getEngineId());

			// parse the original query
			Query originalQuery = QueryFactory.create(this.query, Syntax.syntaxSPARQL_11);
			// create a new query for counting
			Query countQuery = new Query();
			countQuery.setQuerySelectType();
			countQuery.setQueryPattern(originalQuery.getQueryPattern());
			// create the count expression
			ExprAggregator countExpr = new ExprAggregator(null, AggregatorFactory.createCount(false));
			// add the count expression as a result variable
			countQuery.addResultVar("mycount", countExpr);

			String newQuery = countQuery.toString();
			Dataset dataset = null;
			try {
				queryT.setQuery(newQuery);
				queryT.setStartTimeNow();
				Map<String, Object> map = (Map<String, Object>) engine.execQuery(newQuery);
				dataset = (Dataset) map.get(RDFJenaTDBEngine.DATASET_OBJECT);
				ResultSet resultSet = (ResultSet) map.get(RDFJenaTDBEngine.QUERY_RETURN);
				queryT.setEndTimeNow();
				if (resultSet != null && resultSet.hasNext()) {
					QuerySolution row = resultSet.next();
					RDFNode node = row.get("mycount");
					Object cleanValue = getRealValue(node);
					if (cleanValue instanceof Number) {
						this.numRows = ((Number) cleanValue).longValue();
					}
				}
			} catch (Exception e) {
				queryT.setFailed();
				classLogger.error("Error executing count query to determine number of rows", e);
			} finally {
				if (dataset != null) {
					dataset.end();
				}
				Thread.ofVirtual().start(queryT);
			}
		}
		return this.numRows;
	}

	@Override
	public long getNumRecords() {
		return getNumRows() * this.numColumns;
	}

	@Override
	public void reset() throws Exception {
		close();
		execute();
	}

	@Override
	public boolean flushable() {
		return false;
	}

	@Override
	public String flush() {
		return null;
	}
}
