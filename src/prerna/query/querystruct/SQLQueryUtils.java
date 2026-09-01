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
package prerna.query.querystruct;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;

import prerna.ds.nativeframe.NativeFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.SqlInterpreter;
import prerna.query.parsers.SqlParser2;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.reactor.imports.NativeImporter;
import prerna.sablecc2.om.Join;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class SQLQueryUtils {

	private static final Logger classLogger = LogManager.getLogger(SQLQueryUtils.class);

	/**
	 * Merge 2 native frame query structs together based on the joins defined
	 * 
	 * @param curQS
	 * @param qs
	 * @param joins
	 * @return
	 */
	public static NativeFrame joinQueryStructs(SelectQueryStruct curQS, SelectQueryStruct qs, List<Join> joins) {
		// we can do this 2 ways
		// we can do this through genexpression
		// or do it through relationsets
		// with gen expression we can start to move to other pieces

		try {
			IQueryInterpreter interp = curQS.retrieveQueryStructEngine().getQueryInterpreter();
			interp.setQueryStruct(curQS);
			String curQuery = interp.composeQuery();

			interp = qs.retrieveQueryStructEngine().getQueryInterpreter();
			interp.setQueryStruct(qs);
			String thisQuery = interp.composeQuery();

			GenExpression retExpression = joinSQL(List.of(curQuery, thisQuery), joins, List.of(curQS, qs));

			StringBuffer finalOutput = retExpression.printQS(retExpression, null, getQueryUtil(qs.retrieveQueryStructEngine()));

			HardSelectQueryStruct hqs = new HardSelectQueryStruct();
			hqs.customFrom = finalOutput.toString();
			hqs.engineId = qs.engineId;
			hqs.engine = qs.engine;
			hqs.qsType = QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY;
			hqs.setQuery(finalOutput.toString());

			NativeFrame emptyFrame = new NativeFrame();
			NativeImporter importer = new NativeImporter(emptyFrame, hqs);
			importer.insertData();

			return emptyFrame;
		} catch (Exception e) {
			classLogger.error("Failed to join the query structs", e);
		}

		return null;
	}

	public static GenExpression joinSQL(List<String> expressions, List<Join> joins) {
		return joinSQL(expressions, joins, null);
	}

	private static GenExpression joinSQL(List<String> expressions, List<Join> joins,
			List<SelectQueryStruct> sourceQueryStructs) {
		// get the expression
		// add the selectors to the master one
		// and then add the join
		// first one is from
		// and then jointypes

		Map<String, String> aliasTranslationMap = new HashMap<String, String>();

		SqlParser2 parser = new SqlParser2();
		parser.parameterize = false;

		GenExpression retExpression = new GenExpression();
		retExpression.setOperation("select");
		GenExpression lastExpression = null;
		String leftAlias = null;
		String rightAlias = null;
		for (int expIndex = 0; expIndex < expressions.size(); expIndex++) {
			String sql = expressions.get(expIndex);

			GenExpression curExpr = null;
			try {
				SelectQueryStruct sourceQueryStruct = sourceQueryStructs == null ? null
						: sourceQueryStructs.get(expIndex);
				curExpr = parseQuery(parser, sql, sourceQueryStruct);
			} catch (Exception e) {
				classLogger.error("Failed to parse the SQL query {}", sql, e);
			}

			String aliasForThisExpr = Utility.getRandomString(5);
			// get the alias
			String curTableAlias = null;
			if (curExpr.from != null) {
				curTableAlias = curExpr.from.getLeftExpr();
				if (curTableAlias == null) {
					curTableAlias = curExpr.from.leftAlias;
				}
				aliasForThisExpr = aliasForThisExpr + "_" + curTableAlias;
				// add this for the joins later
				aliasTranslationMap.put(curTableAlias, aliasForThisExpr);
			}

			if (leftAlias == null) {
				leftAlias = aliasForThisExpr;
			} else {
				rightAlias = aliasForThisExpr;
			}

			// add these selectors to
			// our main selector
			// I can add all the conditions to even the last one and it would work fine
			if (curExpr.nselectors != null && curExpr.nselectors.size() > 0) {
				List<GenExpression> curSelectors = curExpr.nselectors;

				for (int selectorIndex = 0; selectorIndex < curSelectors.size(); selectorIndex++) {
					GenExpression curSelector = curSelectors.get(selectorIndex);

					// make a copy
					// GenExpression newSelector = makeCopy(curSelector);

					// we could jsut keep the alias instead of the entire selector
					GenExpression newSelector = new GenExpression();
					newSelector.setOperation("column");
					if (curSelector.leftAlias != null && curSelector.leftAlias.length() > 0) {
						newSelector.setLeftExpr(curSelector.leftAlias);
					} else {
						newSelector.setLeftExpr(curSelector.leftExpr);
					}
					newSelector.tableName = aliasForThisExpr;
					newSelector.userTableName = aliasForThisExpr;
					// replace the alias / name of this column this needs to be
					// newSelector.replaceTableAlias(newSelector, curTableAlias, aliasForThisExpr);
					newSelector.aQuery = newSelector.tableName + "." + newSelector.getLeftExpr();

					// done.. now we can add it
					// add it thorugh method
					retExpression.addSelect(newSelector);
					// retExpression.nselectors.add(newSelector);
				}

				// see if this is the first statement. if so make it to be from
				// add this as a from
				GenExpression exprCopy = makeCopy(curExpr);
				lastExpression = exprCopy;
				// exprCopy.paranthesis = true;
				// exprCopy.composite = true;
				exprCopy.leftAlias = aliasForThisExpr;
				exprCopy.setLeftExpr(aliasForThisExpr);

				if (expIndex == 0) {
					retExpression.from = exprCopy;
				} else {
					// add this as a join
					retExpression.joins.add(exprCopy);
					exprCopy.telescope = true;
				}
			}
		}

		// now process the joins
		GenExpression joinExpr = null;
		for (int joinIndex = 0; joinIndex < joins.size(); joinIndex++) {
			// get the join
			// get l columna and r column
			// swap the aliases
			// add it to the last join do the process above
			Join thisJoin = joins.get(joinIndex);
			joinExpr = makeJoin(thisJoin, joinExpr, leftAlias, rightAlias, retExpression.nselectors);
			// remove the duplicate from existing list of selectors
			retExpression.nselectors = removeDuplicateSelectors(thisJoin, retExpression.nselectors, rightAlias);
		}

		realiasDuplicateSelectorNames(retExpression.nselectors);

		retExpression.joins.remove(lastExpression);
		joinExpr.from = lastExpression;
		retExpression.joins.add(joinExpr);

		return retExpression;
	}

	public static GenExpression makeJoin(Join thisJoin, GenExpression lastExpr, String leftAlias, String rightAlias,
			List<GenExpression> nSelectors) {
		// form new body term
		String lColumn = thisJoin.getLColumn(); // this could potentially be other things but for now for isntance this
												// could be a full query
		String rColumn = thisJoin.getRColumn();
		if (lColumn.indexOf("__") > 0) {
			lColumn = lColumn.substring(lColumn.indexOf("__") + 2);
		}
		if (rColumn.indexOf("__") > 0) {
			rColumn = rColumn.substring(rColumn.indexOf("__") + 2);
		}
		lColumn = leftAlias + ".\"" + lColumn + "\"";
		rColumn = rightAlias + ".\"" + rColumn + "\"";

		GenExpression thisJoinBody = new GenExpression();
		String op = thisJoin.getComparator();
		if (op.equals("==")) {
			op = "=";
		}
		thisJoinBody.setOperation(op);
		GenExpression leftColumn = new GenExpression();
		leftColumn.operation = "string";
		leftColumn.leftItem = lColumn;
		GenExpression rightColumn = new GenExpression();
		rightColumn.operation = "string";
		rightColumn.leftItem = rColumn;
		thisJoinBody.leftItem = leftColumn;
		thisJoinBody.rightItem = rightColumn;

		if (lastExpr == null) {
			GenExpression joinExpr = new GenExpression();
			String joinType = thisJoin.getJoinType();
			joinType = joinType.replace(".", " ");
			joinExpr.setOn(joinType);
			joinExpr.telescope = true;
			joinExpr.body = thisJoinBody;

			return joinExpr;
		} else {
			// check for joinType update
			String joinType = thisJoin.getJoinType();
			joinType = joinType.replace(".", " ");
			if (lastExpr.on.equals("left outer join")) {
				if (joinType.equals("right outer join")) {
					joinType = "outer join";
				} else if (joinType.equals("inner join")) {
					joinType = lastExpr.on;
				}
			} else if (lastExpr.on.equals("right outer join")) {
				if (joinType.equals("left outer join")) {
					joinType = "outer join";
				} else if (joinType.equals("inner join")) {
					joinType = lastExpr.on;
				}
			} else if (lastExpr.on.equals("outer join")) {
				joinType = lastExpr.on;
			}
			lastExpr.setOn(joinType);

			// update body tree
			GenExpression newBody = new GenExpression();
			newBody.setOperation("AND");
			newBody.recursive = true;
			newBody.setLeftExpresion(lastExpr.body);
			newBody.setRightExpresion(thisJoinBody);
			lastExpr.body = newBody;

			return lastExpr;
		}
	}

	public static List<GenExpression> removeDuplicateSelectors(Join thisJoin, List<GenExpression> nSelectors,
			String rightAlias) {
		String rColumn = thisJoin.getRColumn();

		if (rColumn.indexOf("__") > 0) {
			rColumn = rColumn.substring(rColumn.indexOf("__") + 2);
		}

		for (int selectorIndex = 0; selectorIndex < nSelectors.size(); selectorIndex++) {
			GenExpression thisColumn = nSelectors.get(selectorIndex);
			String leftExpr = thisColumn.getLeftExpr();
			if (leftExpr != null && leftExpr.startsWith("\"")) {
				leftExpr = leftExpr.replace("\"", "");
				if (leftExpr.equalsIgnoreCase(rColumn) && thisColumn.tableName != null
						&& thisColumn.tableName.equals(rightAlias)) {
					nSelectors.remove(selectorIndex);
				}
			}
		}

		return nSelectors;
	}

	// re-alias selectors with duplicate names irrespective of table
	// this is so custom queries on the built results don't error out with ambiguous
	// column references
	public static void realiasDuplicateSelectorNames(List<GenExpression> nSelectors) {
		Map<String, Integer> aliases = new HashMap<>();

		for (int selectorIndex = 0; selectorIndex < nSelectors.size(); selectorIndex++) {
			GenExpression thisColumn = nSelectors.get(selectorIndex);
			String exprActual = thisColumn.getLeftExpr();
			String exprUniq = exprActual.replace("\"", "");

			Integer exprCt = aliases.get(exprUniq);
			if (exprCt == null) {
				aliases.put(exprUniq, 1);
			} else {
				aliases.put(exprUniq, exprCt++);

				if (exprActual.endsWith("\"")) {
					thisColumn
							.setLeftAlias(exprActual.substring(0, exprActual.length() - 1) + exprCt.toString() + "\"");
				} else {
					thisColumn.setLeftAlias(exprActual + exprCt.toString());
				}
			}
		}
	}

	public static GenExpression makeCopy(GenExpression input) {
		GenExpression retExpression = null;
		ByteArrayOutputStream baos = null;
		FSTObjectOutput fo = null;
		ByteArrayInputStream bais = null;
		FSTObjectInput fi = null;
		try {
			baos = new ByteArrayOutputStream();
			// FST
			fo = new FSTObjectOutput(baos);
			fo.writeObject(input);
			fo.flush();

			byte[] bytes = baos.toByteArray();
			bais = new ByteArrayInputStream(bytes);
			fi = new FSTObjectInput(bais);
			Object retObject = fi.readObject();

			retExpression = (GenExpression) retObject;
		} catch (ClassNotFoundException e) {
			classLogger.error("Failed to deserialize the GenExpression while making a copy", e);
		} catch (IOException e) {
			classLogger.error("Failed to serialize/deserialize the GenExpression while making a copy", e);
		} finally {
			if (baos != null) {
				try {
					baos.close();
				} catch (IOException e) {
					classLogger.error("Error closing the byte array output stream", e);
				}
			}
			if (fo != null) {
				try {
					fo.close();
				} catch (IOException e) {
					classLogger.error("Error closing the FST object output stream", e);
				}
			}
			if (bais != null) {
				try {
					bais.close();
				} catch (IOException e) {
					classLogger.error("Error closing the byte array input stream", e);
				}
			}
			if (fi != null) {
				try {
					fi.close();
				} catch (IOException e) {
					classLogger.error("Error closing the FST object input stream", e);
				}
			}
		}

		return retExpression;
	}

	/**
	 * 
	 * @param subQueryStruct
	 * @param wrapperQueryStruct
	 * @return
	 */
	public static NativeFrame subQuery(SelectQueryStruct subQueryStruct, SelectQueryStruct wrapperQueryStruct) {

		// parse main query
		// convert subquery into GenExpression
		// give the main query and alias
		// confirm the column is there
		// replace the aliases in the subquery
		// push it to NativeFrame call it a day
		NativeFrame emptyFrame = null;

		try {
			// grab the specific interpreter type from the engine itself
			IRDBMSEngine engine = (IRDBMSEngine) subQueryStruct.retrieveQueryStructEngine();
			IQueryInterpreter interp = engine.getQueryInterpreter();

			SqlParser2 parser = new SqlParser2();
			parser.parameterize = false;

			interp.setQueryStruct(subQueryStruct);
			String subQuery = interp.composeQuery();
			GenExpression subQueryExpression = parseQuery(parser, subQuery, subQueryStruct);

			interp = new SqlInterpreter(engine);
			interp.setQueryStruct(wrapperQueryStruct);
			String mainQuery = interp.composeQuery();
			GenExpression mainQueryExpression = parseQuery(parser, mainQuery, wrapperQueryStruct);

			String mainQueryAlias = Utility.getRandomString(5);

			if (mainQueryExpression.from != null) {
				String curAlias = subQueryExpression.from.getLeftExpr();
				mainQueryAlias = mainQueryAlias + "_" + curAlias;
			}

			// now replace the column aliases
			mainQueryExpression.replaceTableAlias2(mainQueryExpression, null, mainQueryAlias);
			mainQueryExpression.addQuoteToColumn(mainQueryExpression, "\"");

			// replace the from
			// and give it an alias
			mainQueryExpression.from = subQueryExpression;
			mainQueryExpression.from.paranthesis = true;
			mainQueryExpression.from.composite = true;
			mainQueryExpression.from.leftAlias = mainQueryAlias;

			StringBuffer finalOutput = mainQueryExpression.printQS(mainQueryExpression, null, engine.getQueryUtil());

			HardSelectQueryStruct hqs = new HardSelectQueryStruct();
			hqs.customFrom = finalOutput.toString();
			hqs.engineId = subQueryStruct.engineId;
			hqs.engine = subQueryStruct.retrieveQueryStructEngine();
			hqs.qsType = QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY;
			hqs.setQuery(finalOutput.toString());

			emptyFrame = new NativeFrame();
			emptyFrame.setName(wrapperQueryStruct.getFrameName());
			NativeImporter importer = new NativeImporter(emptyFrame, hqs);
			importer.insertData();
		} catch (Exception e) {
			classLogger.error("Failed to build the sub query", e);
		}
		return emptyFrame;
	}

	/**
	 * SubQuery on a Native Frame
	 * 
	 * @param queryStruct
	 * @return
	 */
	public static NativeFrame subQueryNativeFrame(SelectQueryStruct queryStruct) {
		NativeFrame emptyFrame = null;
		try {
			IDatabaseEngine engine = queryStruct.retrieveQueryStructEngine();
			IQueryInterpreter interp = engine.getQueryInterpreter();

			SqlParser2 parser = new SqlParser2();
			parser.parameterize = false;

			interp.setQueryStruct(queryStruct);
			String mainQuery = interp.composeQuery();
			GenExpression mainQueryExpression = parseQuery(parser, mainQuery, queryStruct);

			StringBuffer finalOutput = GenExpression.printQS(mainQueryExpression, null, getQueryUtil(engine));

			HardSelectQueryStruct hqs = new HardSelectQueryStruct();
			hqs.customFrom = finalOutput.toString();
			hqs.engineId = queryStruct.engineId;
			hqs.engine = queryStruct.retrieveQueryStructEngine();
			hqs.qsType = QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY;
			hqs.setQuery(finalOutput.toString());

			emptyFrame = new NativeFrame();
			emptyFrame.setName(queryStruct.getFrameName());
			NativeImporter importer = new NativeImporter(emptyFrame, hqs);
			importer.insertData();
		} catch (Exception e) {
			classLogger.error("Failed to build the sub query on the native frame", e);
		}
		return emptyFrame;
	}

	/**
	 * Merge 2 native frame query structs together based on the joins defined
	 * 
	 * @param curQS
	 * @param qs
	 * @param joins
	 * @return
	 */
	public static NativeFrame unionQueryStructs(SelectQueryStruct curQS, SelectQueryStruct qs) {
		// we can do this 2 ways
		// we can do this through genexpression
		// or do it through relationsets
		// with gen expression we can start to move to other pieces

		try {
			IQueryInterpreter interp = curQS.retrieveQueryStructEngine().getQueryInterpreter();
			interp.setQueryStruct(curQS);
			String curQuery = interp.composeQuery();

			interp = qs.retrieveQueryStructEngine().getQueryInterpreter();
			interp.setQueryStruct(qs);
			String thisQuery = interp.composeQuery();

			GenExpression retExpression = unionSQL(List.of(curQuery, thisQuery), List.of(curQS, qs));

			StringBuffer finalOutput = retExpression.printQS(retExpression, null, getQueryUtil(qs.retrieveQueryStructEngine()));

			HardSelectQueryStruct hqs = new HardSelectQueryStruct();
			hqs.customFrom = finalOutput.toString();
			hqs.engineId = qs.engineId;
			hqs.engine = qs.engine;
			hqs.qsType = QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY;
			hqs.setQuery(finalOutput.toString());

			NativeFrame emptyFrame = new NativeFrame();
			NativeImporter importer = new NativeImporter(emptyFrame, hqs);
			importer.insertData();

			return emptyFrame;
		} catch (Exception e) {
			classLogger.error("Failed to union the query structs", e);
		}

		return null;
	}

	private static AbstractSqlQueryUtil getQueryUtil(IDatabaseEngine engine) {
		if (engine instanceof IRDBMSEngine) {
			return ((IRDBMSEngine) engine).getQueryUtil();
		}
		return null;
	}

	/**
	 * 
	 * @param expressions
	 * @return
	 */
	public static GenExpression unionSQL(List<String> expressions) {
		return unionSQL(expressions, null);
	}

	private static GenExpression unionSQL(List<String> expressions, List<SelectQueryStruct> sourceQueryStructs) {
		// get the expression
		// add the selectors to the master one
		// and then add the join
		// first one is from
		// and then jointypes

		Map<String, String> aliasTranslationMap = new HashMap<String, String>();

		SqlParser2 parser = new SqlParser2();
		parser.parameterize = false;

		// need to subquery this as well

		OperationExpression retExpression = new OperationExpression();
		retExpression.setOperation("union");
		retExpression.setComposite(true);
		GenExpression lastExpression = null;

		for (int expIndex = 0; expIndex < expressions.size(); expIndex++) {
			String sql = expressions.get(expIndex);

			GenExpression curExpr = null;
			try {
				SelectQueryStruct sourceQueryStruct = sourceQueryStructs == null ? null
						: sourceQueryStructs.get(expIndex);
				curExpr = parseQuery(parser, sql, sourceQueryStruct);
			} catch (Exception e) {
				classLogger.error("Failed to parse the SQL query {}", sql, e);
			}

			// first one is easy
			if (expIndex == 0) {
				retExpression.operands.add(curExpr);
				retExpression.opNames.add("UNION ALL");
				lastExpression = curExpr;
			} else if (lastExpression.compareSelectors(curExpr)) {
				retExpression.operands.add(curExpr);
			} else {
				// throw an error this cannot be done
			}

		}
		return selfSubQuery(retExpression, lastExpression);
	}

	private static GenExpression parseQuery(SqlParser2 parser, String sql, SelectQueryStruct sourceQueryStruct)
			throws Exception {
		GenExpression expression = parser.processQuery(sql).root;
		if (sourceQueryStruct != null) {
			expression.setLimit(sourceQueryStruct.getLimit());
			expression.setOffSet(sourceQueryStruct.getOffset());
		}
		return expression;
	}

	/**
	 * 
	 * @param innerQuery
	 * @param outerQuery
	 * @return
	 */
	public static GenExpression selfSubQuery(GenExpression innerQuery, GenExpression outerQuery) {
		GenExpression retExpression = new GenExpression();
		retExpression.setOperation("select");

		try {
			// get the selectors from the outer query
			String randomString = Utility.getRandomString(5);
			String subqName = randomString;

			if (innerQuery.from != null) {
				subqName = innerQuery.from.getLeftExpr();
				if (subqName == null) {
					subqName = innerQuery.from.leftAlias;
				}
				subqName = subqName + "_" + randomString;
			}

			for (int selectorIndex = 0; selectorIndex < outerQuery.nselectors.size(); selectorIndex++) {
				GenExpression curSelector = outerQuery.nselectors.get(selectorIndex);
				GenExpression newSelector = new GenExpression();
				newSelector.setOperation("column");
				if (curSelector.leftAlias != null && curSelector.leftAlias.length() > 0) {
					newSelector.setLeftExpr(curSelector.leftAlias);
				} else {
					newSelector.setLeftExpr(curSelector.leftExpr);
				}
				newSelector.tableName = subqName;

				retExpression.addSelect(newSelector);
			}

			// add this as a from now
			retExpression.paranthesis = true;
			retExpression.composite = true;
			retExpression.from = innerQuery;
			retExpression.from.leftAlias = subqName;
		} catch (Exception e) {
			classLogger.error("Failed to build the self sub query", e);
		}
		return retExpression;
	}

}
