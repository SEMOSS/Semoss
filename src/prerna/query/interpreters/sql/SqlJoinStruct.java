package prerna.query.interpreters.sql;

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

}
