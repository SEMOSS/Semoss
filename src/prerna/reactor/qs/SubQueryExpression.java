package prerna.reactor.qs;

import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.transform.QsToPixelConverter;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ITask;
import prerna.util.insight.InsightUtility;

public class SubQueryExpression {

	/*
	 * Just a wrapper around a QS object to be executed 
	 * and the results used within another expression
	 */
	
	private SelectQueryStruct qs = null;
	private transient Insight insight = null;
	private transient String pixelString = null;
	
	public SubQueryExpression() {
		
	}

	public SelectQueryStruct getQs() {
		return qs;
	}

	public void setQs(SelectQueryStruct qs) {
		this.qs = qs;
		this.pixelString = QsToPixelConverter.getPixel(this.qs, true);
	}
	
	public Insight getInsight() {
		return insight;
	}

	public void setInsight(Insight insight) {
		this.insight = insight;
	}
	
	/**
	 * Get the task that is created from the QS
	 * @return
	 */
	public ITask generateQsTask() {
		BasicIteratorTask innerTask = InsightUtility.constructTaskFromQs(this.insight, this.qs);
		return innerTask;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj == null || !(obj instanceof SubQueryExpression)) {
			return false;
		}
		SubQueryExpression other = (SubQueryExpression) obj;
		if(this.qs == null && other.qs == null) {
			return true;
		}
		if(this.qs == null || other.qs == null) {
			return false;
		}
		
		return this.qs.equals(other.qs);
	}
	
	public String getQueryStructName() {
		return " SubQueryExpression(qs=[(" + this.pixelString + ")]) ";
	}
	
}
