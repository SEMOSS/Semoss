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
package prerna.rdf.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;

/**
 * Reads a SQL SELECT and describes it the way the RDF side expects: tables
 * become concepts, projected columns become properties, and each join becomes a
 * triple linking the two concepts it connects.
 * <p>
 * Joins are picked up from two places, an explicit ON clause and an equality
 * between two columns in the WHERE clause, because both express the same
 * relationship. Only SELECT is handled; anything else is logged and ignored.
 */
public class SQLQueryParser extends AbstractQueryParser {

	private static final Logger classLogger = LogManager.getLogger(SQLQueryParser.class);

	public static final String conceptUri = "http://semoss.org/ontologies/Concept/";
	public static final String propertyUri = "http://semoss.org/ontologies/Relation/Contains/";
	private static final String relationUri = "http://semoss.org/ontologies/Relation/";

	// the relationship uri to the {fromTable, fromColumn, toTable, toColumn} it was
	// built from. Keying by the uri is what collapses the duplicate joins that the
	// SEMOSS outer join syntax produces
	private Map<String, String[]> tripleMappings = new HashMap<String, String[]>();
	// every column the WHERE clause mentions
	private Map<String, String> whereClauseVars = new HashMap<String, String>();

	public SQLQueryParser() {
		super();
	}

	public SQLQueryParser(String query) {
		super(query);
	}

	/**
	 * Parse the query into the tables, properties, return variables and triples
	 * that describe it. A query that cannot be parsed is logged and leaves this
	 * parser empty rather than raising.
	 */
	@Override
	public void parseQuery() {

		Statement statement;
		try {
			statement = CCJSqlParserUtil.parse(query);

			// only SELECT is supported; update and insert are not needed here
			if (statement instanceof Select) {
				// the tables have to be resolved first, because the joins below are
				// expressed in terms of aliases that only this pass knows about
				parseTablesAndAlias(statement);
				parseAllPropertiesAndVarsFromQuery(statement);
			} else {
				classLogger.error("An error occurred, the sql statement you are trying to parse is not parseable {}",
						query);
			}
		} catch (JSQLParserException e1) {
			classLogger.error("Error parsing the SQL query", e1);
		}
	}

	/**
	 * Record every table and alias in the statement, and turn the explicit ON
	 * clauses into triples.
	 *
	 * @param statement the SELECT to read
	 * @throws JSQLParserException if the statement cannot be read
	 */
	private void parseTablesAndAlias(Statement statement) throws JSQLParserException {
		HashMap<Column, Column> joinColumnsMap = new HashMap<Column, Column>();
		Select selectStatement = (Select) statement;

		List<PlainSelect> plainSelectList = getPlainSelectList(selectStatement);
		if (plainSelectList != null && plainSelectList.size() > 0) {
			for (PlainSelect ps : plainSelectList) {
				// get the first table in the from clause
				FromItem initialTable = ps.getFromItem();
				if (initialTable instanceof Table) {
					setTableAndAlias((Table) initialTable);

					List<Join> psJoins = ps.getJoins();
					if (psJoins != null) {
						for (Join psJoin : psJoins) {
							// a join can carry several ON expressions, and cross joins carry none at all
							for (Expression exp : psJoin.getOnExpressions()) {
								// TODO: an AndExpression or OrExpression in the ON clause is skipped
								if (exp instanceof EqualsTo) {
									EqualsTo joinExp = (EqualsTo) exp;
									Column leftJoinColumn = (Column) joinExp.getLeftExpression();
									Column rightJoinColumn = (Column) joinExp.getRightExpression();

									// the SEMOSS outer join syntax emits the same join twice, once per
									// side, and the duplicates collapse later when the triples are keyed
									// by their relationship uri
									joinColumnsMap.put(leftJoinColumn, rightJoinColumn);
								}
							}
							// the joined table itself also has to be registered
							FromItem psJoinTable = psJoin.getRightItem();
							if (psJoinTable != null) {
								setTableAndAlias((Table) psJoinTable);
							}
						}
					}
				}
			}
		}

		processAllTableJoins(joinColumnsMap);
	}

	/**
	 * Turn each joined pair of columns into a triple, resolving the alias each
	 * column is qualified by back to its real table.
	 * <p>
	 * So {@code T.Title = N.Title_FK}, where T is Title and N is Nominated, becomes
	 * the relationship {@code Title.Title.Nominated.Title_FK}. Keying by that
	 * relationship is what collapses the two halves of a SEMOSS outer join into one
	 * triple.
	 *
	 * @param joinColumnsMap the left column of each join to its right column
	 */
	private void processAllTableJoins(HashMap<Column, Column> joinColumnsMap) {
		for (Column leftColumn : joinColumnsMap.keySet()) {
			Column rightColumn = joinColumnsMap.get(leftColumn);

			// {fromTable, fromColumn, toTable, toColumn}
			String[] relationships = new String[4];
			relationships[0] = resolveTableName(leftColumn);
			relationships[1] = leftColumn.getColumnName();
			relationships[2] = resolveTableName(rightColumn);
			relationships[3] = rightColumn.getColumnName();

			String relTriple = relationUri + Arrays.toString(relationships).replace(",", ".").replace("[", "")
					.replace("]", "").replaceAll("\\s+", "");
			tripleMappings.put(relTriple, relationships);
		}
	}

	/**
	 * The real table a column is qualified by, which may have been written as an
	 * alias.
	 *
	 * @param column the qualified column
	 * @return the table name behind the qualifier
	 */
	private String resolveTableName(Column column) {
		return aliasTableMap.get(column.getTable().getName());
	}

	/**
	 * Read the WHERE clause of every select for implicit joins, then collect the
	 * projections.
	 *
	 * @param statement the SELECT to read
	 * @throws JSQLParserException if the statement cannot be read
	 */
	private void parseAllPropertiesAndVarsFromQuery(Statement statement) throws JSQLParserException {
		Select selectStatement = (Select) statement;

		List<PlainSelect> plainSelectList = getPlainSelectList(selectStatement);
		if (plainSelectList != null && plainSelectList.size() > 0) {
			for (PlainSelect ps : plainSelectList) {
				if (ps.getWhere() != null) {
					// this also picks up the joins written as a WHERE equality
					getIndividualWhereClauseValues(props, ps.getWhere());
				}
			}
		}
		parseReturnVariables(selectStatement);
	}

	/**
	 * Parse a query in isolation and report just its return variables.
	 *
	 * @param query the SQL to read
	 * @return the return variables, empty when the query cannot be parsed
	 */
	public Map<String, Map<String, String>> getReturnVarsFromQuery(String query) {
		try {
			Select selectStatement = (Select) CCJSqlParserUtil.parse(query);
			parseTablesAndAlias(selectStatement);
			if (!aliasTableMap.isEmpty()) {
				parseReturnVariables(selectStatement);
			}
		} catch (JSQLParserException e) {
			classLogger.error("Error parsing the return variables from the SQL query", e);
		}

		return getReturnVariables();
	}

	/**
	 * Register a table as a concept, and its alias if it has one. The alias is the
	 * key rather than the table name, because the same table can be joined several
	 * times in one query under different aliases.
	 *
	 * @param addTable the table to register
	 */
	private void setTableAndAlias(Table addTable) {
		Alias tableAlias = addTable.getAlias();
		String tableName = addTable.getName();
		if (tableAlias != null) {
			aliasTableMap.put(tableAlias.getName(), tableName);
		}

		types.put(tableName, conceptUri + tableName);
	}

	/**
	 * Collect the projections as return variables, and record the ones that are
	 * plain columns as properties of the table they came from.
	 *
	 * @param statement the SELECT to read
	 * @throws JSQLParserException if the statement cannot be read
	 */
	private void parseReturnVariables(Statement statement) throws JSQLParserException {
		Select selectStatement = (Select) statement;
		List<PlainSelect> plainSelectList = getPlainSelectList(selectStatement);
		if (plainSelectList != null && plainSelectList.size() > 0) {
			for (PlainSelect ps : plainSelectList) {
				List<SelectItem<?>> selectList = ps.getSelectItems();
				for (int i = 0; i < selectList.size(); i++) {
					SelectItem<?> se = selectList.get(i);
					Alias alias = se.getAlias();
					// with no alias the whole select item text stands in as its name
					String expressionAlias = alias != null ? alias.getName() : se.toString();

					Expression expression = se.getExpression();
					returnVariables.add(expression.toString());
					if (expression instanceof Function) {
						hasColumnAggregatorFunction = true;
					}
					// only a plain column can be attributed to a table; an expression or a
					// function has no single column to hang off one
					if (expression instanceof Column) {
						Column returnColumn = (Column) expression;
						String columnName = returnColumn.getColumnName();
						String tableName = resolveTableName(returnColumn);
						addToVariablesMap(typePropVariables, tableName, expressionAlias, columnName);
						addToVariablesMap(typeReturnVariables, tableName, expressionAlias, columnName);
					}
				}
			}
		}
	}

	/**
	 * Walk a WHERE predicate for the joins written as an equality between two
	 * columns, recording every column it mentions along the way.
	 * <p>
	 * A predicate is a left leaning tree, so this descends the left hand side one
	 * comparison at a time and stops once the left hand side is a plain column,
	 * meaning the bottom of the tree has been reached.
	 *
	 * @param props unused, kept for signature compatibility with the callers
	 * @param exp   the predicate to walk
	 */
	private void getIndividualWhereClauseValues(Map<String, String> props, Expression exp) {
		if (exp instanceof ParenthesedExpressionList) {
			ParenthesedExpressionList<?> parenthesed = (ParenthesedExpressionList<?>) exp;
			if (!parenthesed.isEmpty()) {
				getIndividualWhereClauseValues(props, parenthesed.get(0));
			}
			return;
		}
		if (exp instanceof IsNullExpression) {
			// IS NULL carries only a left hand side, so there is no comparison to walk
			Expression nullCheckOn = ((IsNullExpression) exp).getLeftExpression();
			if (nullCheckOn instanceof Column) {
				setWhereClauseDetails((Column) nullCheckOn);
			}
			return;
		}
		if (!(exp instanceof BinaryExpression)) {
			// anything else is a predicate shape this parser does not model
			return;
		}
		BinaryExpression individualExpressions = (BinaryExpression) exp;
		HashMap<Column, Column> joinColumnsMap = new HashMap<Column, Column>();
		while (individualExpressions.getLeftExpression() != null) {
			Expression leftExpression = individualExpressions.getLeftExpression();
			Expression rightExpression = individualExpressions.getRightExpression();
			if (leftExpression instanceof Column && rightExpression instanceof Column) {
				// a column on both sides is a join rather than a value comparison, and is
				// checked against the owl later to decide whether it is a real one
				Column leftJoinColumn = (Column) leftExpression;
				Column rightJoinColumn = (Column) rightExpression;
				setWhereClauseDetails(leftJoinColumn);
				setWhereClauseDetails(rightJoinColumn);

				joinColumnsMap.put(leftJoinColumn, rightJoinColumn);

				if (individualExpressions instanceof EqualsTo) {
					break;
				}
			} else {
				// a literal on the right carries no column to record, so only the shapes
				// that hold one are followed
				if (rightExpression instanceof Column) {
					setWhereClauseDetails((Column) rightExpression);
				} else if (rightExpression instanceof IsNullExpression) {
					Expression expNull = ((IsNullExpression) rightExpression).getLeftExpression();
					if (expNull instanceof Column) {
						setWhereClauseDetails((Column) expNull);
					}
				} else if (rightExpression instanceof BinaryExpression
						|| rightExpression instanceof ParenthesedExpressionList) {
					// the other half of a conjunction, which is a predicate in its own right
					getIndividualWhereClauseValues(props, rightExpression);
				}

				// a column on the left means this is a leaf comparison rather than a
				// conjunction, so there is nothing further down the left to walk
				if (leftExpression instanceof Column) {
					setWhereClauseDetails((Column) leftExpression);
					break;
				}
			}
			Expression nextLeft = individualExpressions.getLeftExpression();
			if (!(nextLeft instanceof BinaryExpression)) {
				break;
			}
			individualExpressions = (BinaryExpression) nextLeft;
		}
		processAllTableJoins(joinColumnsMap);

	}

	/**
	 * Record a column referenced by the WHERE clause, keyed by the qualifier it was
	 * written under.
	 *
	 * @param columnDetail the column to record
	 */
	private void setWhereClauseDetails(Column columnDetail) {
		String columnName = columnDetail.getColumnName();
		Table tableName = columnDetail.getTable();
		if (tableName == null) {
			// an unqualified column, so there is no table to key it against
			return;
		}
		String tableFullName = tableName.getName();
		Alias tableAlias = tableName.getAlias();
		String tableAliasText = tableFullName;
		if (tableAlias != null) {
			tableAliasText = tableAlias.getName();
		}
		whereClauseVars.put(tableAliasText + "__" + columnName, propertyUri + columnName);
	}

	/**
	 * Flatten a statement into the individual selects it is made of, so that a
	 * union is read the same way as a single select.
	 *
	 * @param selectStatement the statement to flatten
	 * @return each PlainSelect in it, empty for a shape holding none
	 */
	private List<PlainSelect> getPlainSelectList(Select selectStatement) {
		List<PlainSelect> plainSelectList = new ArrayList<PlainSelect>();
		if (selectStatement instanceof SetOperationList) {
			for (Select s : ((SetOperationList) selectStatement).getSelects()) {
				if (s instanceof PlainSelect) {
					plainSelectList.add((PlainSelect) s);
				}
			}
		} else if (selectStatement instanceof PlainSelect) {
			plainSelectList.add((PlainSelect) selectStatement);
		}
		return plainSelectList;
	}

	/**
	 * The joins as {fromConcept, relationship, toConcept} triples. The columns the
	 * join was built from are part of the relationship uri rather than separate
	 * entries.
	 *
	 * @return one triple per distinct join
	 */
	@Override
	public List<String[]> getTriplesData() {
		for (String key : tripleMappings.keySet()) {
			String[] mapping = tripleMappings.get(key);

			String[] triple = new String[3];
			triple[0] = conceptUri + mapping[0];
			triple[1] = key;
			triple[2] = conceptUri + mapping[2];

			triplesData.add(triple);
		}
		return triplesData;
	}

}