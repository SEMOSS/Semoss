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
package prerna.query.parsers;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NextValExpression;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.StringProvider;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Block;
import net.sf.jsqlparser.statement.Commit;
import net.sf.jsqlparser.statement.DeclareStatement;
import net.sf.jsqlparser.statement.DescribeStatement;
import net.sf.jsqlparser.statement.ExplainStatement;
import net.sf.jsqlparser.statement.SetStatement;
import net.sf.jsqlparser.statement.ShowColumnsStatement;
import net.sf.jsqlparser.statement.ShowStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.UseStatement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.comment.Comment;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.AlterView;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.ParenthesedFromItem;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.Values;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.show.ShowIndexStatement;
import net.sf.jsqlparser.statement.show.ShowTablesStatement;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;
import net.sf.jsqlparser.statement.upsert.Upsert;
import net.sf.jsqlparser.statement.upsert.UpsertType;
import net.sf.jsqlparser.util.TablesNamesFinder;

/**
 * Parses SQL into a policy-oriented inventory using the project's JSQLParser
 * dependency. No keyword or regular-expression fallback is used: an unsupported
 * dialect construct remains a parser failure for the guardrail policy to
 * decide.
 */
public final class SqlQueryPolicyParser {

	// The SQL AST represents these unparenthesized context expressions as
	// unqualified Column nodes. Only promote recognized context expressions;
	// ordinary and explicitly delimited column identifiers remain columns.
	private static final Set<String> UNQUALIFIED_KEYWORD_EXPRESSIONS = Set.of("CURRENT_CATALOG", "CURRENT_DATE",
			"CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME",
			"CURRENT_TIMESTAMP", "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURRENT_USER", "LOCALTIME", "LOCALTIMESTAMP",
			"SESSION_USER", "SYSTEM_USER", "USER");

	private SqlQueryPolicyParser() {
	}

	public static Analysis parse(String query, boolean squareBracketQuotation) throws Exception {
		CCJSqlParser parser = new CCJSqlParser(new StringProvider(query));
		parser.withSquareBracketQuotation(squareBracketQuotation);
		parser.setErrorRecovery(false);
		Statements parsed = parser.Statements();

		Analysis analysis = new Analysis();
		analysis.statementCount = parsed.size();
		for (Statement statement : parsed) {
			inspectStatement(statement, analysis);
		}
		return analysis;
	}

	private static void inspectStatement(Statement statement, Analysis analysis) {
		if (statement instanceof Values) {
			analysis.operations.add(Operation.VALUES);
		} else if (statement instanceof Select) {
			analysis.operations.add(Operation.SELECT);
			inspectSelect((Select) statement, analysis);
		} else if (statement instanceof Insert) {
			Insert insert = (Insert) statement;
			analysis.operations.add(Operation.INSERT);
			List<UpdateSet> duplicateUpdateSets = insert.getDuplicateUpdateSets();
			if (duplicateUpdateSets != null && !duplicateUpdateSets.isEmpty()) {
				analysis.operations.add(Operation.UPSERT);
			}
			inspectSelect(insert.getSelect(), analysis);
		} else if (statement instanceof Update) {
			Update update = (Update) statement;
			analysis.operations.add(Operation.UPDATE);
			if (update.getWhere() == null) {
				analysis.updateWithoutWhere = true;
			}
			inspectUpdateSets(update.getUpdateSets(), analysis);
			inspectDmlFromAndJoins(update.getFromItem(), update.getStartJoins(), update.getJoins(), analysis);
		} else if (statement instanceof Delete) {
			Delete delete = (Delete) statement;
			analysis.operations.add(Operation.DELETE);
			if (delete.getWhere() == null) {
				analysis.deleteWithoutWhere = true;
			}
			inspectDmlFromAndJoins(null, null, delete.getJoins(), analysis);
		} else if (statement instanceof Merge) {
			Merge merge = (Merge) statement;
			analysis.operations.add(Operation.MERGE);
			inspectDmlFromAndJoins(merge.getFromItem(), null, null, analysis);
		} else if (statement instanceof Upsert) {
			Upsert upsert = (Upsert) statement;
			analysis.operations.add(isReplace(upsert) ? Operation.REPLACE : Operation.UPSERT);
			inspectSelect(upsert.getSelect(), analysis);
		} else if (statement instanceof Truncate) {
			analysis.operations.add(Operation.TRUNCATE);
		} else if (statement instanceof CreateTable) {
			analysis.operations.add(Operation.CREATE_TABLE);
			CreateTable create = (CreateTable) statement;
			if (create.getSelect() != null) {
				inspectSelect(create.getSelect(), analysis);
			}
		} else if (statement instanceof CreateView) {
			analysis.operations.add(Operation.CREATE_VIEW);
			CreateView create = (CreateView) statement;
			if (create.getSelect() != null) {
				inspectSelect(create.getSelect(), analysis);
			}
		} else if (statement instanceof CreateIndex) {
			analysis.operations.add(Operation.CREATE_INDEX);
		} else if (statement instanceof AlterView) {
			analysis.operations.add(Operation.ALTER_VIEW);
		} else if (statement instanceof Alter) {
			analysis.operations.add(Operation.ALTER_TABLE);
		} else if (statement instanceof Drop) {
			analysis.operations.add(Operation.DROP);
		} else if (statement instanceof Comment) {
			analysis.operations.add(Operation.COMMENT);
		} else if (statement instanceof Execute) {
			analysis.operations.add(Operation.EXECUTE);
			addIdentifier(analysis.routines, ((Execute) statement).getName());
		} else if (statement instanceof SetStatement) {
			analysis.operations.add(Operation.SET);
		} else if (statement instanceof UseStatement) {
			analysis.operations.add(Operation.USE);
		} else if (statement instanceof Commit) {
			analysis.operations.add(Operation.COMMIT);
		} else if (statement instanceof DeclareStatement) {
			analysis.operations.add(Operation.DECLARE);
		} else if (statement instanceof ShowColumnsStatement || statement instanceof ShowStatement
				|| statement instanceof ShowTablesStatement || statement instanceof ShowIndexStatement) {
			analysis.operations.add(Operation.SHOW);
		} else if (statement instanceof DescribeStatement) {
			analysis.operations.add(Operation.DESCRIBE);
		} else if (statement instanceof ExplainStatement) {
			analysis.operations.add(Operation.EXPLAIN);
			inspectSelect(((ExplainStatement) statement).getStatement(), analysis);
		} else if (statement instanceof Block) {
			analysis.operations.add(Operation.BLOCK);
			Statements nested = ((Block) statement).getStatements();
			if (nested != null) {
				for (Statement nestedStatement : nested) {
					inspectStatement(nestedStatement, analysis);
				}
			}
		} else {
			analysis.operations.add(Operation.UNKNOWN);
		}

		inspectIdentifiers(statement, analysis);
	}

	private static boolean isReplace(Upsert upsert) {
		UpsertType upsertType = upsert.getUpsertType();
		return upsertType == UpsertType.REPLACE || upsertType == UpsertType.REPLACE_SET
				|| upsertType == UpsertType.INSERT_OR_REPLACE;
	}

	private static void inspectSelect(Select select, Analysis analysis) {
		if (select == null) {
			return;
		}
		new SelectShapeInspector(analysis).inspect(select);
	}

	/**
	 * An assigned value can itself be a subquery, so every SET value is inspected
	 * for select shape rather than only the first one.
	 */
	private static void inspectUpdateSets(List<UpdateSet> updateSets, Analysis analysis) {
		if (updateSets == null) {
			return;
		}
		for (UpdateSet updateSet : updateSets) {
			if (updateSet.getValues() == null) {
				continue;
			}
			for (Expression value : updateSet.getValues()) {
				if (value instanceof Select) {
					inspectSelect((Select) value, analysis);
				}
			}
		}
	}

	private static void inspectDmlFromAndJoins(net.sf.jsqlparser.statement.select.FromItem fromItem,
			List<Join> startJoins, List<Join> joins, Analysis analysis) {
		SelectShapeInspector inspector = new SelectShapeInspector(analysis);
		if (fromItem != null) {
			fromItem.accept(inspector.fromItemVisitor, null);
		}
		inspector.inspectJoins(startJoins);
		inspector.inspectJoins(joins);
	}

	private static void inspectIdentifiers(Statement statement, Analysis analysis) {
		AstInventory inventory = new AstInventory();
		List<String> tables = new ArrayList<>();
		try {
			if (statement instanceof Select || statement instanceof Delete || statement instanceof Update
					|| statement instanceof Insert || statement instanceof Truncate || statement instanceof CreateTable
					|| statement instanceof Merge || statement instanceof Upsert || statement instanceof Comment
					|| statement instanceof DescribeStatement || statement instanceof ExplainStatement) {
				tables.addAll(inventory.getTables(statement));
			} else if (statement instanceof CreateView) {
				CreateView create = (CreateView) statement;
				addTable(tables, create.getView());
				if (create.getSelect() != null) {
					// Select is both a Statement and an Expression, so the overload has to be
					// pinned down
					tables.addAll(inventory.getTables((Statement) create.getSelect()));
				}
			} else if (statement instanceof AlterView) {
				addTable(tables, ((AlterView) statement).getView());
			} else if (statement instanceof CreateIndex) {
				addTable(tables, ((CreateIndex) statement).getTable());
			} else if (statement instanceof Alter) {
				addTable(tables, ((Alter) statement).getTable());
			} else if (statement instanceof Drop) {
				addTable(tables, ((Drop) statement).getName());
			} else if (statement instanceof Execute) {
				Execute execute = (Execute) statement;
				ExpressionList<?> arguments = execute.getExprList();
				if (arguments != null) {
					for (Expression expression : arguments) {
						inventory.getTables(expression);
					}
				}
			} else if (statement instanceof SetStatement) {
				SetStatement set = (SetStatement) statement;
				for (int i = 0; i < set.getCount(); i++) {
					List<Expression> expressions = set.getExpressions(i);
					if (expressions != null) {
						for (Expression expression : expressions) {
							inventory.getTables(expression);
						}
					}
				}
			} else if (statement instanceof DeclareStatement) {
				UserVariable variable = ((DeclareStatement) statement).getUserVariable();
				if (variable != null) {
					inventory.visit(variable, null);
				}
			}
		} catch (UnsupportedOperationException e) {
			// The operation is still represented by its Statement subclass. JSQLParser's
			// table finder does not support every DDL node.
		}

		for (String table : tables) {
			addIdentifier(analysis.relations, table);
		}
		analysis.functions.addAll(inventory.functions);
		analysis.variables.addAll(inventory.variables);
		analysis.keywords.addAll(inventory.keywords);
	}

	private static void addTable(List<String> tables, Table table) {
		if (table != null) {
			tables.add(table.getFullyQualifiedName());
		}
	}

	private static void addIdentifier(Set<String> destination, String identifier) {
		if (identifier != null && !identifier.trim().isEmpty()) {
			destination.add(canonicalIdentifier(identifier));
		}
	}

	static String canonicalIdentifier(String identifier) {
		return identifier.replace("\"", "").replace("`", "").replace("[", "").replace("]", "").trim()
				.toUpperCase(Locale.ROOT);
	}

	private static boolean isDelimitedIdentifier(String identifier) {
		if (identifier == null) {
			return false;
		}
		String trimmed = identifier.trim();
		return trimmed.length() >= 2 && ((trimmed.startsWith("\"") && trimmed.endsWith("\""))
				|| (trimmed.startsWith("`") && trimmed.endsWith("`"))
				|| (trimmed.startsWith("[") && trimmed.endsWith("]")));
	}

	private static boolean isUnqualifiedKeywordExpression(String identifier) {
		return identifier != null && !isDelimitedIdentifier(identifier)
				&& UNQUALIFIED_KEYWORD_EXPRESSIONS.contains(canonicalIdentifier(identifier));
	}

	public enum OperationGroup {
		READ, METADATA, WRITE, DDL, ROUTINE, SESSION, TRANSACTION, LOCK, UNKNOWN
	}

	public enum Operation {
		SELECT(OperationGroup.READ), VALUES(OperationGroup.READ), EXPLAIN(OperationGroup.READ),
		SHOW(OperationGroup.METADATA), DESCRIBE(OperationGroup.METADATA), INSERT(OperationGroup.WRITE),
		UPDATE(OperationGroup.WRITE), DELETE(OperationGroup.WRITE), MERGE(OperationGroup.WRITE),
		UPSERT(OperationGroup.WRITE), REPLACE(OperationGroup.WRITE), CREATE_TABLE(OperationGroup.DDL),
		CREATE_VIEW(OperationGroup.DDL), CREATE_INDEX(OperationGroup.DDL), ALTER_TABLE(OperationGroup.DDL),
		ALTER_VIEW(OperationGroup.DDL), DROP(OperationGroup.DDL), TRUNCATE(OperationGroup.DDL),
		COMMENT(OperationGroup.DDL), SELECT_INTO(OperationGroup.DDL), EXECUTE(OperationGroup.ROUTINE),
		BLOCK(OperationGroup.ROUTINE), SET(OperationGroup.SESSION), USE(OperationGroup.SESSION),
		DECLARE(OperationGroup.SESSION), COMMIT(OperationGroup.TRANSACTION), SELECT_FOR_UPDATE(OperationGroup.LOCK),
		UNKNOWN(OperationGroup.UNKNOWN);

		public final OperationGroup group;

		Operation(OperationGroup group) {
			this.group = group;
		}
	}

	public static final class Analysis {
		public int statementCount;
		public final Set<Operation> operations = new LinkedHashSet<>();
		public final Set<String> functions = new LinkedHashSet<>();
		public final Set<String> variables = new LinkedHashSet<>();
		public final Set<String> keywords = new LinkedHashSet<>();
		public final Set<String> relations = new LinkedHashSet<>();
		public final Set<String> routines = new LinkedHashSet<>();
		public boolean deleteWithoutWhere;
		public boolean updateWithoutWhere;
		public boolean selectStar;
		public boolean cartesianJoin;
		public boolean recursiveCte;
		public int joinCount;
	}

	private static final class AstInventory extends TablesNamesFinder<Void> {
		private final Set<String> functions = new LinkedHashSet<>();
		private final Set<String> variables = new LinkedHashSet<>();
		private final Set<String> keywords = new LinkedHashSet<>();

		@Override
		public <S> Void visit(Function function, S context) {
			addIdentifier(functions, function.getName());
			return super.visit(function, context);
		}

		@Override
		public <S> Void visit(UserVariable variable, S context) {
			addIdentifier(variables, variable.toString());
			return null;
		}

		@Override
		public <S> Void visit(Column column, S context) {
			if (column.getTable() == null || column.getTable().getName() == null
					|| column.getTable().getName().isBlank()) {
				String columnName = column.getColumnName();
				if (isUnqualifiedKeywordExpression(columnName)) {
					addIdentifier(keywords, columnName);
				}
			}
			return super.visit(column, context);
		}

		@Override
		public <S> Void visit(TableFunction tableFunction, S context) {
			if (tableFunction.getFunction() != null) {
				tableFunction.getFunction().accept(this, context);
			}
			return null;
		}

		@Override
		public <S> Void visit(AnalyticExpression analytic, S context) {
			addIdentifier(functions, analytic.getName());
			if (analytic.getExpression() != null) {
				analytic.getExpression().accept(this, context);
			}
			return null;
		}

		@Override
		public <S> Void visit(NextValExpression nextVal, S context) {
			addIdentifier(functions, "NEXTVAL");
			return null;
		}

		@Override
		public <S> Void visit(TimeKeyExpression keyword, S context) {
			addIdentifier(keywords, keyword.getStringValue());
			return null;
		}
	}

	private static final class SelectShapeInspector extends SelectVisitorAdapter<Void> {
		private final Analysis analysis;
		private final FromItemVisitorAdapter<Void> fromItemVisitor = new FromItemVisitorAdapter<Void>() {
			@Override
			public <S> Void visit(ParenthesedSelect parenthesedSelect, S context) {
				inspectParenthesedSelect(parenthesedSelect);
				return null;
			}

			@Override
			public <S> Void visit(LateralSubSelect lateralSubSelect, S context) {
				inspectParenthesedSelect(lateralSubSelect);
				return null;
			}

			@Override
			public <S> Void visit(ParenthesedFromItem parenthesedFromItem, S context) {
				if (parenthesedFromItem.getFromItem() != null) {
					parenthesedFromItem.getFromItem().accept(this, context);
				}
				inspectJoins(parenthesedFromItem.getJoins());
				return null;
			}
		};

		private SelectShapeInspector(Analysis analysis) {
			this.analysis = analysis;
		}

		private void inspect(Select select) {
			if (select.getWithItemsList() != null) {
				for (WithItem<?> withItem : select.getWithItemsList()) {
					visit(withItem, null);
				}
			}
			select.accept(this, null);
		}

		@Override
		public <S> Void visit(PlainSelect select, S context) {
			if (select.getIntoTables() != null && !select.getIntoTables().isEmpty()) {
				analysis.operations.add(Operation.SELECT_INTO);
			}
			if (select.getForMode() != null) {
				analysis.operations.add(Operation.SELECT_FOR_UPDATE);
			}
			if (select.getSelectItems() != null) {
				// AllTableColumns extends AllColumns, so both t.* and * land on this check
				analysis.selectStar |= select.getSelectItems().stream().map(SelectItem::getExpression)
						.anyMatch(expression -> expression instanceof AllColumns);
			}
			if (select.getFromItem() != null) {
				select.getFromItem().accept(fromItemVisitor, context);
			}
			inspectJoins(select.getJoins());
			return null;
		}

		@Override
		public <S> Void visit(ParenthesedSelect parenthesedSelect, S context) {
			inspectParenthesedSelect(parenthesedSelect);
			return null;
		}

		@Override
		public <S> Void visit(SetOperationList setOperationList, S context) {
			if (setOperationList.getSelects() != null) {
				for (Select body : setOperationList.getSelects()) {
					body.accept(this, context);
				}
			}
			return null;
		}

		@Override
		public <S> Void visit(WithItem<?> withItem, S context) {
			analysis.recursiveCte |= withItem.isRecursive();
			if (withItem.getSelect() != null) {
				withItem.getSelect().accept(this, context);
			}
			return null;
		}

		private void inspectParenthesedSelect(ParenthesedSelect parenthesedSelect) {
			if (parenthesedSelect.getWithItemsList() != null) {
				for (WithItem<?> withItem : parenthesedSelect.getWithItemsList()) {
					visit(withItem, null);
				}
			}
			if (parenthesedSelect.getSelect() != null) {
				parenthesedSelect.getSelect().accept(this, null);
			}
		}

		private void inspectJoins(List<Join> joins) {
			if (joins == null) {
				return;
			}
			analysis.joinCount += joins.size();
			for (Join join : joins) {
				// getOnExpressions and getUsingColumns are never null, so absence is an empty
				// list
				analysis.cartesianJoin |= join.isCross()
						|| (join.isSimple() && join.getOnExpressions().isEmpty() && join.getUsingColumns().isEmpty());
				if (join.getRightItem() != null) {
					join.getRightItem().accept(fromItemVisitor, null);
				}
			}
		}
	}
}
