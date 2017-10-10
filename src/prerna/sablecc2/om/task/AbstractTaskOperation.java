package prerna.sablecc2.om.task;

import prerna.engine.api.IHeadersDataRow;

public abstract class AbstractTaskOperation extends AbstractTask {

	protected transient ITask innerTask;
	
	public AbstractTaskOperation() {
		
	}
	
	public AbstractTaskOperation(ITask innerTask) {
		setInnerTask(innerTask);
	}
	
	/**
	 * Get all the props from the original task
	 * @param innerTask
	 */
	private void consumeInnerTask(ITask innerTask) {
		this.formatter = innerTask.getFormatter();
		this.taskOptions = innerTask.getTaskOptions();
		this.headerInfo = innerTask.getHeaderInfo();
		this.sortInfo = innerTask.getSortInfo();
	}
	
	@Override
	public boolean hasNext() {
		// base implementation
		return this.innerTask.hasNext();
	}
	
	@Override
	public IHeadersDataRow next() {
		// base implementation
		return this.innerTask.next();
	}
	
	@Override
	public void cleanUp() {
		this.innerTask.cleanUp();
	}
	
	@Override
	public void reset() {
		this.innerTask.reset();
	}
	
	public void setInnerTask(ITask innerTask) {
		this.innerTask = innerTask;
		consumeInnerTask(innerTask);
	}
}
