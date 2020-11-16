package prerna.util.gson;

import com.google.gson.TypeAdapter;

import prerna.om.Insight;

public abstract class AbstractSemossTypeAdapter<T> extends TypeAdapter<T> {

	/*
	 * Abstract class to set the insight
	 */
	
	protected Insight insight = null;

	/**
	 * Set the insight for context on read
	 * @param insight
	 */
	public void setInsight(Insight insight) {
		this.insight = insight;
	}
}
