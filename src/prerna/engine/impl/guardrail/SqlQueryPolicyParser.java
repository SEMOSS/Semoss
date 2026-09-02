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
package prerna.engine.impl.guardrail;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NextValExpression;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.UserVariable;
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
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.ParenthesisFromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.upsert.Upsert;
import net.sf.jsqlparser.statement.values.ValuesStatement;
import net.sf.jsqlparser.util.TablesNamesFinder;

/**
 * Parses SQL into a policy-oriented inventory using the project's JSQLParser
 * dependency. No keyword or regular-expression fallback is used: an unsupported
 * dialect construct remains a parser failure for the guardrail policy to
 * decide.
 */
final class SqlQueryPolicyParser {

	// The SQL AST represents these unparenthesized context expressions as
	// unqualified Column nodes. Only promote recognized context expressions;
	// ordinary and explicitly delimited column identifiers remain columns.
	private static final Set<String> UNQUALIFIED_KEYWORD_EXPRESSIONS = Set.of("CURRENT_CATALOG", "CURRENT_DATE",
			"CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME",
			"CURRENT_TIMESTAMP", "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURRENT_USER", "LOCALTIME", "LOCALTIMESTAMP",
			"SESSION_USER", "SYSTEM_USER", "USER");

	private SqlQueryPolicyParser() {
	}

	static Analysis parse(String query, boolean squareBracketQuotation) throws Exception {
		CCJSqlParser parser = new CCJSqlParser(new StringProvider(query));
		parser.withSquareBracketQuotation(squareBracketQuotation);
		parser.setErrorRecovery(false);
		Statements parsed = parser.Statements();

		Analysis analysis = new Analysis();
		analysis.statementCount = parsed.getStatements().size();
		for (Statement statement : parsed.getStatements()) {
			inspectStatement(statement, analysis);
		}
		return analysis;
	}

	private static void inspectStatement(Statement statement, Analysis analysis) {
		if (statement instanceof Select) {
			analysis.operations.add(Operation.SELECT);
			inspectSelect((Select) statement, analysis);
		} else if (statement instanceof Insert) {
			Insert insert = (Insert) statement;
			analysis.operations.add(Operation.INSERT);
			if (insert.isUseDuplicate()) {
				analysis.operations.add(Operation.UPSERT);
			}
			inspectSelect(insert.getSelect(), analysis);
		} else if (statement instanceof Update) {
			Update update = (Update) statement;
			analysis.operations.add(Operation.UPDATE);
			if (update.getWhere() == null) {
				analysis.updateWithoutWhere = true;
			}
			inspectSelect(update.getSelect(), analysis);
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
			if (merge.getUsingSelect() != null) {
				inspectSelectBody(merge.getUsingSelect().getSelectBody(), analysis);
			}
		} else if (statement instanceof Upsert) {
			analysis.operations.add(Operation.UPSERT);
			inspectSelect(((Upsert) statement).getSelect(), analysis);
		} else if (statement instanceof Replace) {
			analysis.operations.add(Operation.REPLACE);
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
		} else if (statement instanceof ShowColumnsStatement || statement instanceof ShowStatement) {
			analysis.operations.add(Operation.SHOW);
		} else if (statement instanceof DescribeStatement) {
			analysis.operations.add(Operation.DESCRIBE);
		} else if (statement instanceof ExplainStatement) {
			analysis.operations.add(Operation.EXPLAIN);
			inspectSelect(((ExplainStatement) statement).getStatement(), analysis);
		} else if (statement instanceof ValuesStatement) {
			analysis.operations.add(Operation.VALUES);
		} else if (statement instanceof Block) {
			analysis.operations.add(Operation.BLOCK);
			Statements nested = ((Block) statement).getStatements();
			if (nested != null) {
				for (Statement nestedStatement : nested.getStatements()) {
					inspectStatement(nestedStatement, analysis);
				}
			}
		} else {
			analysis.operations.add(Operation.UNKNOWN);
		}

		inspectIdentifiers(statement, analysis);
	}

	private static void inspectSelect(Select select, Analysis analysis) {
		if (select == null) {
			return;
		}
		SelectShapeInspector inspector = new SelectShapeInspector(analysis);
		if (select.getWithItemsList() != null) {
			for (WithItem withItem : select.getWithItemsList()) {
				withItem.accept(inspector);
			}
		}
		if (select.getSelectBody() != null) {
			select.getSelectBody().accept(inspector);
		}
	}

	private static void inspectSelectBody(SelectBody selectBody, Analysis analysis) {
		if (selectBody != null) {
			selectBody.accept(new SelectShapeInspector(analysis));
		}
	}

	private static void inspectDmlFromAndJoins(net.sf.jsqlparser.statement.select.FromItem fromItem,
			List<Join> startJoins, List<Join> joins, Analysis analysis) {
		SelectShapeInspector inspector = new SelectShapeInspector(analysis);
		if (fromItem != null) {
			fromItem.accept(inspector.fromItemVisitor);
		}
		inspector.inspectJoins(startJoins);
		inspector.inspectJoins(joins);
	}

	private static void inspectIdentifiers(Statement statement, Analysis analysis) {
		AstInventory inventory = new AstInventory();
		List<String> tables = new ArrayList<>();
		try {
			if (statement instanceof Select || statement instanceof Delete || statement instanceof Update
					|| statement instanceof Insert || statement instanceof Replace || statement instanceof Truncate
					|| statement instanceof CreateTable || statement instanceof Merge || statement instanceof Upsert
					|| statement instanceof ValuesStatement || statement instanceof Comment
					|| statement instanceof DescribeStatement || statement instanceof ExplainStatement) {
				tables.addAll(inventory.getTableList(statement));
			} else if (statement instanceof CreateView) {
				CreateView create = (CreateView) statement;
				addTable(tables, create.getView());
				if (create.getSelect() != null) {
					tables.addAll(inventory.getTableList(create.getSelect()));
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
				if (execute.getExprList() != null) {
					for (net.sf.jsqlparser.expression.Expression expression : execute.getExprList().getExpressions()) {
						inventory.getTableList(expression);
					}
				}
			} else if (statement instanceof SetStatement) {
				SetStatement set = (SetStatement) statement;
				for (int i = 0; i < set.getCount(); i++) {
					if (set.getExpression(i) != null) {
						inventory.getTableList(set.getExpression(i));
					}
				}
			} else if (statement instanceof DeclareStatement) {
				UserVariable variable = ((DeclareStatement) statement).getUserVariable();
				if (variable != null) {
					inventory.visit(variable);
				}
			}
		} catch (UnsupportedOperationException e) {
			// The operation is still represented by its Statement subclass. JSQLParser
			// 3.1's table finder does not support every DDL node.
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

	enum OperationGroup {
		READ, METADATA, WRITE, DDL, ROUTINE, SESSION, TRANSACTION, LOCK, UNKNOWN
	}

	enum Operation {
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

		final OperationGroup group;

		Operation(OperationGroup group) {
			this.group = group;
		}
	}

	static final class Analysis {
		int statementCount;
		final Set<Operation> operations = new LinkedHashSet<>();
		final Set<String> functions = new LinkedHashSet<>();
		final Set<String> variables = new LinkedHashSet<>();
		final Set<String> keywords = new LinkedHashSet<>();
		final Set<String> relations = new LinkedHashSet<>();
		final Set<String> routines = new LinkedHashSet<>();
		boolean deleteWithoutWhere;
		boolean updateWithoutWhere;
		boolean selectStar;
		boolean cartesianJoin;
		boolean recursiveCte;
		int joinCount;
	}

	private static final class AstInventory extends TablesNamesFinder {
		private final Set<String> functions = new LinkedHashSet<>();
		private final Set<String> variables = new LinkedHashSet<>();
		private final Set<String> keywords = new LinkedHashSet<>();

		@Override
		public void visit(Function function) {
			addIdentifier(functions, function.getName());
			super.visit(function);
		}

		@Override
		public void visit(UserVariable variable) {
			addIdentifier(variables, variable.toString());
		}

		@Override
		public void visit(Column column) {
			if (column.getTable() == null || column.getTable().getName() == null
					|| column.getTable().getName().isBlank()) {
				String columnName = column.getColumnName();
				if (isUnqualifiedKeywordExpression(columnName)) {
					addIdentifier(keywords, columnName);
				}
			}
			super.visit(column);
		}

		@Override
		public void visit(TableFunction tableFunction) {
			if (tableFunction.getFunction() != null) {
				tableFunction.getFunction().accept(this);
			}
		}

		@Override
		public void visit(AnalyticExpression analytic) {
			addIdentifier(functions, analytic.getName());
			if (analytic.getExpression() != null) {
				analytic.getExpression().accept(this);
			}
		}

		@Override
		public void visit(NextValExpression nextVal) {
			addIdentifier(functions, "NEXTVAL");
		}

		@Override
		public void visit(TimeKeyExpression keyword) {
			addIdentifier(keywords, keyword.getStringValue());
		}
	}

	private static final class SelectShapeInspector extends SelectVisitorAdapter {
		private final Analysis analysis;
		private final FromItemVisitorAdapter fromItemVisitor = new FromItemVisitorAdapter() {
			@Override
			public void visit(SubSelect subSelect) {
				inspectSubSelect(subSelect);
			}

			@Override
			public void visit(LateralSubSelect lateralSubSelect) {
				if (lateralSubSelect.getSubSelect() != null) {
					inspectSubSelect(lateralSubSelect.getSubSelect());
				}
			}

			@Override
			public void visit(SubJoin subJoin) {
				if (subJoin.getLeft() != null) {
					subJoin.getLeft().accept(this);
				}
				inspectJoins(subJoin.getJoinList());
			}

			@Override
			public void visit(ParenthesisFromItem parenthesis) {
				if (parenthesis.getFromItem() != null) {
					parenthesis.getFromItem().accept(this);
				}
			}
		};

		private SelectShapeInspector(Analysis analysis) {
			this.analysis = analysis;
		}

		@Override
		public void visit(PlainSelect select) {
			if (select.getIntoTables() != null && !select.getIntoTables().isEmpty()) {
				analysis.operations.add(Operation.SELECT_INTO);
			}
			if (select.isForUpdate()) {
				analysis.operations.add(Operation.SELECT_FOR_UPDATE);
			}
			if (select.getSelectItems() != null) {
				analysis.selectStar |= select.getSelectItems().stream()
						.anyMatch(item -> item instanceof AllColumns || item instanceof AllTableColumns);
			}
			if (select.getFromItem() != null) {
				select.getFromItem().accept(fromItemVisitor);
			}
			inspectJoins(select.getJoins());
		}

		@Override
		public void visit(SetOperationList setOperationList) {
			if (setOperationList.getSelects() != null) {
				for (SelectBody body : setOperationList.getSelects()) {
					body.accept(this);
				}
			}
		}

		@Override
		public void visit(WithItem withItem) {
			analysis.recursiveCte |= withItem.isRecursive();
			if (withItem.getSelectBody() != null) {
				withItem.getSelectBody().accept(this);
			}
		}

		private void inspectSubSelect(SubSelect subSelect) {
			if (subSelect.getWithItemsList() != null) {
				for (WithItem withItem : subSelect.getWithItemsList()) {
					withItem.accept(this);
				}
			}
			if (subSelect.getSelectBody() != null) {
				subSelect.getSelectBody().accept(this);
			}
		}

		private void inspectJoins(List<Join> joins) {
			if (joins == null) {
				return;
			}
			analysis.joinCount += joins.size();
			for (Join join : joins) {
				analysis.cartesianJoin |= join.isCross()
						|| (join.isSimple() && join.getOnExpression() == null && join.getUsingColumns() == null);
				if (join.getRightItem() != null) {
					join.getRightItem().accept(fromItemVisitor);
				}
			}
		}
	}
}
