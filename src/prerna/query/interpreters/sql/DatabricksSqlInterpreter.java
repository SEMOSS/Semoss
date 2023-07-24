package prerna.query.interpreters.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabase;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector.ORDER_BY_DIRECTION;

public class DatabricksSqlInterpreter extends SqlInterpreter {

	private Map<String, String> qsSelectorToAlias = new HashMap<>();
	
	public DatabricksSqlInterpreter() {

	}

	public DatabricksSqlInterpreter(IDatabase engine) {
		super(engine);
	}

	public DatabricksSqlInterpreter(ITableDataFrame frame) {
		super(frame);
	}

	@Override
	public void addSelector(IQuerySelector selector) {
		String alias = selector.getAlias();
		String newSelector = processSelector(selector, true) + " AS `" + alias + "`";
		if(selectors.length() == 0) {
			selectors = newSelector;
		} else {
			selectors += " , " + newSelector;
		}
		selectorList.add(newSelector);
		selectorAliases.add(alias);
		
		this.qsSelectorToAlias.put(selector.getQueryStructName(), alias);
	}
	
	@Override
	protected void addOrderBySelector() {
		int counter = 0;
		for(StringBuilder orderBySelector : this.orderBySelectors) {
			String alias = "o"+counter++;
			String newSelector = "("+orderBySelector+") AS " + "\""+alias+"\"";
			if(selectors.length() == 0) {
				selectors = newSelector;
			} else {
				selectors += " , " + newSelector;
			}
			selectorList.add(newSelector);
			selectorAliases.add(alias);
		}
	}

	@Override
	public StringBuilder appendOrderBy(StringBuilder query) {
		//grab the order by and get the corresponding display name for that order by column
		List<IQuerySort> orderByList = ((SelectQueryStruct) this.qs).getCombinedOrderBy();
		List<StringBuilder> validOrderBys = new ArrayList<>();
		for(IQuerySort orderBy : orderByList) {
			if(orderBy.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.COLUMN) {
				QueryColumnOrderBySelector orderBySelector = (QueryColumnOrderBySelector) orderBy;
				String tableConceptualName = orderBySelector.getTable();
				String columnConceptualName = orderBySelector.getColumn();
				ORDER_BY_DIRECTION orderByDir = orderBySelector.getSortDir();

				boolean origPrim = false;
				if(columnConceptualName.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)){
					origPrim = true;
					columnConceptualName = getPrimKey4Table(tableConceptualName);
				} else if(this.customFromAliasName==null || this.customFromAliasName.isEmpty()){
					columnConceptualName = getPhysicalPropertyNameFromConceptualName(tableConceptualName, columnConceptualName);
				}

				StringBuilder thisOrderBy = new StringBuilder();

				// might want to order by a derived column being returned
				if(origPrim && this.selectorAliases.contains(tableConceptualName)) {
					// either instantiate the string builder or add a comma for multi sort
					if(queryUtil.isSelectorKeyword(tableConceptualName)) {
						thisOrderBy.append(queryUtil.getEscapeKeyword(tableConceptualName));
					} else {
						thisOrderBy.append(queryUtil.escapeReferencedAlias(tableConceptualName));
					}
				}
				// account for custom from + sort is a valid column being returned
				else if(this.customFromAliasName != null && !this.customFromAliasName.isEmpty()) {
					String orderByTable = this.customFromAliasName;
					String orderByColumn = queryUtil.escapeReferencedAlias(columnConceptualName);

					if(this.retTableToCols.get(orderByTable).contains(orderByColumn)) {
						thisOrderBy.append(orderByTable).append(".").append(orderByColumn);
					} else {
						continue;
					}
				}
				// account for sort being on table/column being returned
				else if(this.retTableToCols.containsKey(tableConceptualName) && 
						this.retTableToCols.get(tableConceptualName).contains(columnConceptualName) &&
						orderBy.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.COLUMN && 
						this.qsSelectorToAlias.containsKey( ((QueryColumnOrderBySelector) orderBy).getQueryStructName()) ) 
				{
					// we need to find the alias for this column and order by the alias
					String alias = this.qsSelectorToAlias.get(((QueryColumnOrderBySelector) orderBy).getQueryStructName());
					thisOrderBy.append("`").append(alias).append("`");
				}
				// well, this is not a valid order by to add
				else {
					continue;
				}

				if(orderByDir == ORDER_BY_DIRECTION.ASC) {
					thisOrderBy.append(" ASC ");
				} else {
					thisOrderBy.append(" DESC ");
				}
				validOrderBys.add(thisOrderBy);
			}
		}

		int size = validOrderBys.size();
		for(int i = 0; i < size; i++) {
			if(i == 0) {
				query.append(" ORDER BY ");
			} else {
				query.append(", ");
			}
			query.append(validOrderBys.get(i).toString());
		}
		return query;
	}

}
