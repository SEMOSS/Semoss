package prerna.query.querystruct;

import java.util.List;
import java.util.Map;
import java.util.Vector;

public class WhenExpression extends GenExpression {
	
	// sets the condition
	String elseClause = null;
	
	List <String> whens = new Vector<String>();
	List <String> thens = new Vector<String>();
	
	List <GenExpression> whenE = new Vector <GenExpression>();
	List <GenExpression> thenE = new Vector <GenExpression>();
	GenExpression elseE = null;
	
	public void addWhenThen(String when, String then)
	{
		whens.add(when);
		thens.add(then);
	}
	
	public void setElse(String elseClause)
	{
		this.elseClause = elseClause;
	}
	
	public void addWhenThenE(GenExpression when, GenExpression then)
	{
		whenE.add(when);
		thenE.add(then);

	}
	
	public void setElseE(GenExpression elseE)
	{
		this.elseE = elseE;
	}
	
	public StringBuffer printOutput2()
	{
		StringBuffer output = new StringBuffer();
		
		if(whens.size() > 0)
		{
			output.append("case ");
			
			for(int whenIndex = 0;whenIndex < whens.size();whenIndex++)
			{
				output.append("when " ).append(whens.get(whenIndex)).append(" then ").append(thens.get(whenIndex)).append(" ");
			}
			
			if(elseClause != null)
			{
				output.append(" else ").append(elseClause);
			}
			output.append(" end");
			
			if(this.leftAlias != null)
				output.append(" AS " + this.leftAlias);
		}
		
		return output;
	}

	public StringBuffer printOutput()
	{
		StringBuffer output = new StringBuffer();
		//System.err.println(" Processing >> " + aQuery);
		
		if(whenE.size() > 0)
		{
			output.append("case ");
			
			for(int whenIndex = 0;whenIndex < whenE.size();whenIndex++)
			{
				//System.err.println("When ...  " + whens.get(whenIndex));
				//System.err.println("Then ...  " + thens.get(whenIndex));
				GenExpression thisWhen = whenE.get(whenIndex);
				StringBuffer whenBuf = new StringBuffer();
				whenBuf = thisWhen.printQS(thisWhen, whenBuf);

				GenExpression thisThen = thenE.get(whenIndex);
				StringBuffer thenBuf = new StringBuffer();
				thenBuf = thisThen.printQS(thisThen, thenBuf);


				output.append("when " ).append(whenBuf).append(" then ").append(thenBuf).append(" ");
			}
			
			if(elseE != null)
			{
				StringBuffer elseBuf = new StringBuffer();
				elseBuf = elseE.printQS(elseE, elseBuf);
				output.append(" else ").append(elseBuf);
			}
			output.append(" end");
			
			if(this.leftAlias != null)
				output.append(" AS " + this.leftAlias);
		}
		
		return output;
	}
	
	public static StringBuffer printLevel(GenExpression qs, 
			List <String> realTables, // this is where all the real tables are kept
			int level, 
			GenExpression derivedKey, 
			Map <GenExpression, List <GenExpression>> derivedColumns,
			Map <Integer, List <GenExpression>> levelProjections, // all the columns in a given level
			Map <String, List <GenExpression>> tableColumns, // all the columns in a given level do we need this ?
			Map <String, String> aliases, // aliases for this column - also need something that will keep // also keeps aliases for table
			String tableName,
			boolean derived, 
			boolean projection)
	{
		StringBuffer output = new StringBuffer();
		//System.err.println(" Processing >> " + aQuery);
		WhenExpression we = (WhenExpression)qs;
				
		if(we.whenE.size() > 0)
		{
			output.append("case ");
			
			for(int whenIndex = 0;whenIndex < we.whenE.size();whenIndex++)
			{
				//System.err.println("When ...  " + whens.get(whenIndex));
				//System.err.println("Then ...  " + thens.get(whenIndex));
				GenExpression thisWhen = we.whenE.get(whenIndex);
				thisWhen.printLevel2(thisWhen, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns, aliases, tableName, true, false);

				GenExpression thisThen = we.thenE.get(whenIndex);
				thisThen.printLevel2(thisThen, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns, aliases, tableName, true, false);

			}
			
			if(we.elseE != null)
			{
				printLevel2(we.elseE, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns, aliases, tableName, true, false);
			}
			output.append(" end");			
		}
		
		return output;
	}

	
	

	public void replaceTableAlias2(GenExpression qs, String oldName, String newName)
	{
		StringBuffer output = new StringBuffer();
		
		if(whens.size() > 0)
		{
			output.append("case ");
			
			for(int whenIndex = 0;whenIndex < whenE.size();whenIndex++)
			{
				GenExpression thisWhen = whenE.get(whenIndex);
				thisWhen.replaceTableAlias2(thisWhen, oldName, newName);

				GenExpression thisThen = thenE.get(whenIndex);
				thisWhen.replaceTableAlias2(thisThen, oldName, newName);
			}
			
			if(elseE != null)
			{
				elseE.replaceTableAlias2(elseE, oldName, newName);
			}
		}		
	}
	

	public void addQuoteToColumn(GenExpression qs, String quote)
	{
		StringBuffer output = new StringBuffer();
		
		if(whens.size() > 0)
		{
			output.append("case ");
			
			for(int whenIndex = 0;whenIndex < whenE.size();whenIndex++)
			{
				GenExpression thisWhen = whenE.get(whenIndex);
				thisWhen.addQuoteToColumn(thisWhen, quote);

				GenExpression thisThen = thenE.get(whenIndex);
				thisWhen.addQuoteToColumn(thisThen, quote);
			}
			
			if(elseE != null)
			{
				elseE.addQuoteToColumn(elseE, quote);
			}
		}		
	}



}
