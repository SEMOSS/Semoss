package prerna.query.interpreters.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class SqlJoinStruct {
	
	private String joinType;
	
	private String sourceTable;
	private String sourceTableAlias;
	private String sourceCol;
	
	private String targetTable;
	private String targetTableAlias;
	private String targetCol;
	
	private String comparator = "=";
	
	// specific values for setting a join to an inner query
	// basically completely overriding the original functionality
	private boolean useSubQuery = false;
	private String subQuery;
	private String subQueryAlias;
	private List<String[]> joinOnList = new ArrayList<>();
	
	public SqlJoinStruct() {
		
	}
	
	/**
	 * Returns list of size 2
	 * first index is the source table and source alias
	 * second index is the target table and target alias
	 * @return
	 */
	public List<String[]> getTables() {
		List<String[]> tablesUsed = new Vector<String[]>();
		tablesUsed.add(new String[]{sourceTable, getSourceTableAlias()});
		tablesUsed.add(new String[]{targetTable, getTargetTableAlias()});
		return tablesUsed;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof SqlJoinStruct) {
			SqlJoinStruct otherStruct = (SqlJoinStruct) obj;
			// if they are different types - not equal
			if(otherStruct.useSubQuery != this.useSubQuery) {
				return false;
			}
			
			if(otherStruct.useSubQuery && this.useSubQuery) {
				if(otherStruct.subQueryAlias.equals(this.subQueryAlias)) {
					return true;
				}
				return false;
			} else {
				if(this.joinType.equals(otherStruct.joinType)
						// same source
						&& this.sourceTable.equals(otherStruct.sourceTable)
						&& this.sourceCol.equals(otherStruct.sourceCol)
						// same target
						&& this.targetTable.equals(otherStruct.targetTable)
						&& this.targetCol.equals(otherStruct.targetCol)) 
				{
					// now test the alias
					if(getSourceTableAlias().equals(otherStruct.getSourceTableAlias()) 
							&& getTargetTableAlias().equals(otherStruct.getTargetTableAlias())) 
					{
						return true;
					}
				}
			}
		}
		return false;
	}
	
	/**
	 * Switch the start and to information for the join
	 */
	public void reverse() {
		String tempTargetTable = sourceTable;
		String tempTargetTableAlias = sourceTableAlias;
		String tempTargetCol = sourceCol;
		
		sourceTable = targetTable;
		sourceTableAlias = targetTableAlias;
		sourceCol = targetCol;
		
		targetTable = tempTargetTable;
		targetTableAlias = tempTargetTableAlias;
		targetCol = tempTargetCol;
		
		joinType = getReverseJoinType();
	}
	
	/**
	 * Get the reverse join type
	 * @return
	 */
	public String getReverseJoinType() {
		// if left -> switch to right
		// if right -> switch to left
		// else -> stay same
		if(joinType.contains("left")) {
			return "right outer join";
		} else if(joinType.contains("right")) {
			return "left outer join";
		}
		
		return joinType;
	}
	
	/////////////////////////////////////////////////////////////////////
	
	/*
	 * Setters and Getters
	 */
	
	public String getJoinType() {
		return joinType;
	}

	public void setJoinType(String joinType) {
		this.joinType = joinType;
	}

	public String getSourceTable() {
		return sourceTable;
	}

	public void setSourceTable(String sourceTable) {
		this.sourceTable = sourceTable;
	}

	public String getSourceTableAlias() {
		if(this.sourceTableAlias == null) {
			return sourceTable;
		}
		return sourceTableAlias;
	}

	public void setSourceTableAlias(String sourceTableAlias) {
		this.sourceTableAlias = sourceTableAlias;
	}

	public String getSourceCol() {
		return sourceCol;
	}

	public void setSourceCol(String sourceCol) {
		this.sourceCol = sourceCol;
	}

	public String getTargetTable() {
		return targetTable;
	}

	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
	}

	public String getTargetTableAlias() {
		if(this.targetTableAlias == null) {
			return targetTable;
		}
		return targetTableAlias;
	}

	public void setTargetTableAlias(String targetTableAlias) {
		this.targetTableAlias = targetTableAlias;
	}

	public String getTargetCol() {
		return targetCol;
	}

	public void setTargetCol(String targetCol) {
		this.targetCol = targetCol;
	}

	public String getComparator() {
		return comparator;
	}

	public void setComparator(String comparator) {
		if(comparator != null) {
			if("==".equals(comparator)) {
				this.comparator = "=";
			} else {
				this.comparator = comparator;
			}
		}
	}

	///////////////////////////////////////////////////
	
	// subquery methods
	
	public boolean isUseSubQuery() {
		return useSubQuery;
	}

	public void setUseSubQuery(boolean useSubQuery) {
		this.useSubQuery = useSubQuery;
	}
	
	public String getSubQuery() {
		if(!useSubQuery) {
			throw new IllegalArgumentException("Cannot use this method when useSubquery is false");
		}
		return subQuery;
	}

	public void setSubQuery(String subQuery) {
		if(!useSubQuery) {
			throw new IllegalArgumentException("Cannot use this method when useSubquery is false");
		}
		this.subQuery = subQuery;
	}

	public String getSubQueryAlias() {
		if(!useSubQuery) {
			throw new IllegalArgumentException("Cannot use this method when useSubquery is false");
		}
		return subQueryAlias;
	}

	public void setSubQueryAlias(String subQueryAlias) {
		if(!useSubQuery) {
			throw new IllegalArgumentException("Cannot use this method when useSubquery is false");
		}
		this.subQueryAlias = subQueryAlias;
	}

	public void addJoinOnList(String[] joinOn) {
		if(!useSubQuery) {
			throw new IllegalArgumentException("Cannot use this method when useSubquery is false");
		}
		
		this.joinOnList.add(joinOn);
	}

	public List<String[]> getJoinOnList() {
		return joinOnList;
	}

	public void setJoinOnList(List<String[]> joinOnList) {
		this.joinOnList = joinOnList;
	}
	
}
