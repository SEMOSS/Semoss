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
package prerna.reactor.shortcuts.fileupload.job;

public class ConditionEvaluator {
	/*
	 * private static final JexlEngine engine = new JexlBuilder().create();
	 * 
	 * public static boolean evaluate(String expression, Map<String, Object>
	 * context) {
	 * 
	 * if (expression == null) { return false; }
	 * 
	 * expression = expression.replace("${", "").replace("}", "");
	 * 
	 * JexlExpression expr = engine.createExpression(expression);
	 * 
	 * JexlContext jc = new MapContext(context);
	 * 
	 * Object result = expr.evaluate(jc);
	 * 
	 * return Boolean.TRUE.equals(result); }
	 * 
	 * public static boolean evaluateCondition(Map<String, Object> ctx, Condition
	 * condition) {
	 * 
	 * Object actualValue = resolvePath(ctx, condition.getKey());
	 * 
	 * if (actualValue == null) { return false; }
	 * 
	 * String expected = condition.getValue();
	 * 
	 * switch (condition.getOperator()) {
	 * 
	 * case "EQUALS": return actualValue.toString().equalsIgnoreCase(expected);
	 * 
	 * case "NOT_EQUALS": return !actualValue.toString().equalsIgnoreCase(expected);
	 * 
	 * case "CONTAINS": return actualValue.toString().contains(expected);
	 * 
	 * case "GT": return Double.parseDouble(actualValue.toString()) >
	 * Double.parseDouble(expected);
	 * 
	 * case "LT": return Double.parseDouble(actualValue.toString()) <
	 * Double.parseDouble(expected);
	 * 
	 * default: return false; } }
	 * 
	 * public static Object resolvePath(Map<String, Object> ctx, String path) {
	 * 
	 * // try direct match first if (ctx.containsKey(path)) { return ctx.get(path);
	 * }
	 * 
	 * String[] parts = path.split("\\.");
	 * 
	 * // find longest matching key for (int i = parts.length; i > 0; i--) {
	 * 
	 * String prefix = String.join(".", Arrays.copyOfRange(parts, 0, i));
	 * 
	 * if (ctx.containsKey(prefix)) {
	 * 
	 * Object current = ctx.get(prefix);
	 * 
	 * // resolve remaining fields for (int j = i; j < parts.length; j++) {
	 * 
	 * if (current == null) { return null; }
	 * 
	 * String fieldName = parts[j];
	 * 
	 * try { Field field = current.getClass().getDeclaredField(fieldName);
	 * 
	 * field.setAccessible(true);
	 * 
	 * current = field.get(current);
	 * 
	 * } catch (Exception e) { return null; } }
	 * 
	 * return current; } }
	 * 
	 * return null; }
	 */
}
