package prerna.sablecc2.reactor;

import java.util.List;

import prerna.sablecc2.SimpleTable;

public class TablePKSLPlanner extends PKSLPlanner {

	public static final String OPERATION_COLUMN = "OP";
	public static final String NOUN_COLUMN = "NOUN";
	public static final String DIRECTION_COLUMN = "DIRECTION";
	public static final String PROCESSED_COLUMN = "PROCESSED";

	public static final String OP_TYPE = "Varchar(16000)";
	public static final String NOUN_TYPE = "Varchar(250)";
	public static final String DIRECTION_TYPE = "Varchar(3)"; //either 'IN' or 'OUT'
	public static final String PROCESSED_TYPE = "BOOLEAN";

	public static final String inDirection = "IN";
	public static final String outDirection = "OUT";
	
	public static final String TRUE_PROCESSED = "TRUE";
	public static final String FALSE_PROCESSED = "FALSE";
	
	private SimpleTable table = null;
	
	public TablePKSLPlanner() {
		// TODO: WHY ARE WE CALLING SUPER AND MAKING AN EMPTY TINKER???
		super();
		this.table = new SimpleTable();
		this.table.convertFromInMemToPhysical(null);
		this.table.createTable(this.table.getTableName(), getHeaders(), getTypes());
	}
	
	@Override
	public void addOutputs(String opName, List <String> outputs, IReactor.TYPE opType)
	{
		String[] headers = getHeaders();
		String[] types = getTypes();
		
		for(String output : outputs) {
			String[] values = new String[4];
			values[0] = opName;
			values[1] = output;
			values[2] = outDirection;
			values[3] = FALSE_PROCESSED;
			this.table.addRow(values, headers, types);
		}
	}
	
	@Override
	public void addInputs(String opName, List <String> inputs, List <String> asInputs, IReactor.TYPE opType)
	{
		String[] headers = getHeaders();
		String[] types = getTypes();
		
		for(String input : inputs) {
			String[] values = new String[4];
			values[0] = opName;
			values[1] = input;
			values[2] = inDirection;
			values[3] = FALSE_PROCESSED;
			this.table.addRow(values, headers, types);
		}
	}
	
	public SimpleTable getSimpleTable() {
		return this.table;
	}
	
	public void setSimpleTable(SimpleTable table) {
		this.table = table;
	}
	
	private String[] getHeaders() {
		return new String[]{OPERATION_COLUMN, NOUN_COLUMN, DIRECTION_COLUMN, PROCESSED_COLUMN};
	}
	
	private String[] getTypes() {
		return new String[]{OP_TYPE, NOUN_TYPE, DIRECTION_TYPE, PROCESSED_TYPE};
	}
}
