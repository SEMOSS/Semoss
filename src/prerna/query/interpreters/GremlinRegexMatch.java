/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.query.interpreters;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;

public class GremlinRegexMatch implements PBiPredicate<Object, Object> {

	Pattern pattern = null;
	private Mode mode;

	enum Mode {
		FIND, MATCH
	}

	public GremlinRegexMatch(String regex) {
		this(regex, Mode.FIND);
	}

	public GremlinRegexMatch(String regex, Mode mode) {
		this.mode = mode;
		pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
	}

	@Override
	public boolean test(final Object first, final Object second) {
		String str = first.toString();
		Matcher matcher = pattern.matcher(str);
		switch (mode) {
			case FIND :
				return matcher.find();
			case MATCH :
				return matcher.matches();
		}
		return false;
	}

	/**
	 * get a Regular expression predicate
	 *
	 * @param regex
	 * @return - the predicate
	 */
	public static P<Object> regex(Object regex) {
		PBiPredicate<Object, Object> b = new GremlinRegexMatch(regex.toString());
		return new P<Object>(b, regex);
	}
}
