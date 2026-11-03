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
package prerna.auth.access;

/**
 * One rule of a model usage limit, such as 100,000 tokens a minute for each
 * person or $500 a month shared by a team. A call is allowed only when every
 * rule of the limit that applies to it has usage left.
 *
 * <ul>
 * <li>{@link Metric}: tokens, cost in millionths of a US dollar, or compute
 * time in milliseconds.</li>
 * <li>amount: in the metric's unit. 0 means no limit defined.</li>
 * <li>{@link Window} and its length: a minute window is 1 to
 * {@value #MAX_MINUTES} minutes and an hour window 1 to {@value #MAX_HOURS}
 * hours, and both slide; a day, week, month or year is a fixed UTC calendar
 * window with no length. A length of 1 is kept as null, in memory and in
 * {@code MODELUSAGELIMITRULE.WINDOWLENGTH}.</li>
 * <li>{@link Counts}: each person's own use, or shared by everyone the limit
 * covers. Only a team's limit, public use and the totals can be shared.</li>
 * </ul>
 */
public class ModelUsageLimitRule {

	/** What a rule counts */
	public enum Metric {
		/** Every token the provider reports, prompt and response */
		TOKENS,
		/** Millionths of a US dollar, at the model's known prices */
		COST,
		/** Milliseconds the model spends answering */
		COMPUTE_TIME
	}

	/**
	 * How long a rule's count runs before it starts again. Minute and hour slide,
	 * and can be several minutes or hours long ({@link #getWindowLength()}); day
	 * and longer are fixed UTC calendar windows.
	 */
	public enum Window {
		MINUTE, HOUR, DAY, WEEK, MONTH, YEAR
	}

	/** The longest a minute window can be, in minutes; longer is an hour window */
	public static final int MAX_MINUTES = 59;
	/** The longest an hour window can be, in hours */
	public static final int MAX_HOURS = 24;

	/** Whose use a rule counts */
	public enum Counts {
		/** Each person's own use, against their own allowance */
		EACH_PERSON,
		/** Every call billed to the team, the public use or the model, together */
		SHARED
	}

	private Metric metric;
	/** Tokens, millionths of a dollar or milliseconds */
	private Long amount;
	private Window window;
	/**
	 * How many minutes or hours a minute or hour window is, such as 10 for every 10
	 * minutes; null for 1
	 */
	private Integer windowLength;
	private Counts counts;

	public ModelUsageLimitRule() {

	}

	/**
	 * Creates a rule over a window of length 1, such as one minute or one day.
	 *
	 * @param metric what the rule counts
	 * @param amount tokens, millionths of a dollar or milliseconds
	 * @param window how long the count runs
	 * @param counts whose use it counts
	 */
	public ModelUsageLimitRule(Metric metric, Long amount, Window window, Counts counts) {
		this.metric = metric;
		this.amount = amount;
		this.window = window;
		this.counts = counts;
	}

	/**
	 * Creates a rule over a minute or hour window that is several minutes or hours
	 * long. A length of 1 is kept as null.
	 *
	 * @param metric       what the rule counts
	 * @param amount       tokens, millionths of a dollar or milliseconds
	 * @param window       {@link Window#MINUTE} or {@link Window#HOUR}
	 * @param windowLength how many minutes or hours the window is
	 * @param counts       whose use it counts
	 */
	public ModelUsageLimitRule(Metric metric, Long amount, Window window, int windowLength, Counts counts) {
		this(metric, amount, window, counts);
		this.windowLength = windowLength == 1 ? null : windowLength;
	}

	public Metric getMetric() {
		return metric;
	}

	public Long getAmount() {
		return amount;
	}

	public Window getWindow() {
		return window;
	}

	/**
	 * The window's length in minutes or hours, read as 1 when it is not set or is
	 * less than 1.
	 *
	 * @return how many minutes or hours the window is, 1 unless a minute or hour
	 *         window says otherwise
	 */
	public int getWindowLength() {
		return windowLength == null || windowLength < 1 ? 1 : windowLength;
	}

	/**
	 * The window's length exactly as it was set, without reading a missing or
	 * invalid one as 1, so validation can check it.
	 *
	 * @return the length as sent or stored, null for 1
	 */
	public Integer getWindowLengthOrNull() {
		return windowLength;
	}

	public Counts getCounts() {
		return counts;
	}
}
