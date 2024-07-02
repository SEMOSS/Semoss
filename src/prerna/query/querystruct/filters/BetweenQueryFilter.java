package prerna.query.querystruct.filters;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;

import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.Constants;

public class BetweenQueryFilter implements IQueryFilter, Serializable {

	private static final Logger classLogger = LogManager.getLogger(BetweenQueryFilter.class);

	IQuerySelector column = null;
	Object start = null;
	Object end = null;
	
	public void setColumn(IQuerySelector column)
	{
		this.column = column;
	}
	
	public IQuerySelector getColumn()
	{
		return column;
	}
	
	public void setStart(Object start)
	{
		this.start = start;
	}
	
	public Object getStart()
	{
		return this.start;
	}
	
	public void setEnd(Object end)
	{
		this.end = end;
	}
	
	public Object getEnd()
	{
		return this.end;
	}
	
	
	@Override
	public QUERY_FILTER_TYPE getQueryFilterType() {
		// TODO Auto-generated method stub
		return QUERY_FILTER_TYPE.BETWEEN;
	}

	@Override
	public Set<String> getAllUsedColumns() {
		// TODO Auto-generated method stub
		// assumes it is a column for now..
		Set <String> retSet = new HashSet<String>();
		retSet.add(((QueryColumnSelector)column).getColumn());
		return retSet;
	}
	
	@Override
	public List<QueryColumnSelector> getAllQueryColumns() {
		List<QueryColumnSelector> usedCol = new Vector<>();
		usedCol.add(((QueryColumnSelector)column));
		return usedCol;
	}

	@Override
	public Set<String> getAllQueryStructNames() {
		Set <String> retSet = new HashSet<String>();
		retSet.add(((QueryColumnSelector)column).getColumn());
		return retSet;
	}

	@Override
	public Set<String> getAllUsedTables() {
		// TODO Auto-generated method stub
		Set <String> retSet = new HashSet<String>();
		retSet.add(((QueryColumnSelector)column).getTable());
		return retSet;
	}

	@Override
	public boolean containsColumn(String column) {
		// TODO Auto-generated method stub
		return (((QueryColumnSelector)this.column).getColumn().equalsIgnoreCase(column));
		// we do not know if the objects are same
		
	}

	@Override
	public IQueryFilter copy() {
		// TODO Auto-generated method stub
		IQueryFilter retExpression = null;
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			// FST
			FSTObjectOutput fo = new FSTObjectOutput(baos);
			fo.writeObject(this);
			fo.close();

			byte [] bytes = baos.toByteArray();
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			/*
			FSTObjectInput fi = new FSTObjectInput(bais);
			Object retObject = fi.readObject();
			*/
			FSTObjectInput fi = new FSTObjectInput(bais);
			Object retObject = fi.readObject();

			retExpression = (IQueryFilter)retObject;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
			
		return retExpression;
	}

	@Override
	public String getStringRepresentation() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getSimpleFormat() {
		// TODO Auto-generated method stub
		return null;
	}

}
