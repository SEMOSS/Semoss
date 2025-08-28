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
package prerna.reactor.export.mustache;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.SafeMustacheFactory;
import com.github.mustachejava.reflect.ReflectionObjectHandler;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Array;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MustacheUtility {

  private MustacheUtility() {}

  public static String compile(String template, Map<String, Object> variables) throws Exception {
    if (variables == null || variables.isEmpty()) {
      return template;
    }
    ReflectionObjectHandler oh =
        new ReflectionObjectHandler() {
          @Override
          public Object coerce(final Object object) {
            if (object != null && object.getClass().isArray()) {
              return new ArrayMap(object);
            } else if (object instanceof List) {
              return new ListMap(object);
            } else if (object instanceof Collection) {
              return new ListMap(new ArrayList<Object>((Collection) object));
            }
            return super.coerce(object);
          }
        };
    DefaultMustacheFactory mf = new SafeMustacheFactory(new HashSet<>(), "");
    mf.setObjectHandler(oh);
    Mustache m = mf.compile(new StringReader(template), "template");
    Writer writer = new StringWriter();
    m.execute(writer, variables).flush();
    return writer.toString();
  }

  private static class ArrayMap extends AbstractMap<Object, Object> implements Iterable<Object> {
    private final Object object;

    public ArrayMap(Object object) {
      this.object = object;
    }

    @Override
    public Object get(Object key) {
      try {
        int index = Integer.parseInt(key.toString());
        return Array.get(object, index);
      } catch (NumberFormatException nfe) {
        return null;
      }
    }

    @Override
    public boolean containsKey(Object key) {
      return get(key) != null;
    }

    @Override
    public Set<Entry<Object, Object>> entrySet() {
      Set<Entry<Object, Object>> retSet = new LinkedHashSet<>();
      Iterator<Object> it = this.iterator();
      int counter = 0;
      while (it.hasNext()) {
        Object val = it.next();
        Entry<Object, Object> e = new AbstractMap.SimpleEntry<>(counter++, val);
        retSet.add(e);
      }

      return retSet;
    }

    /**
     * Returns an iterator over a set of elements of type T.
     *
     * @return an Iterator.
     */
    @Override
    public Iterator<Object> iterator() {
      return new Iterator<Object>() {

        int index = 0;
        int length = Array.getLength(object);

        @Override
        public boolean hasNext() {
          return index < length;
        }

        @Override
        public Object next() {
          return Array.get(object, index++);
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  private static class ListMap extends AbstractMap<Object, Object> implements Iterable<Object> {
    private final Object object;

    public ListMap(Object object) {
      this.object = object;
    }

    @Override
    public Object get(Object key) {
      try {
        int index = Integer.parseInt(key.toString());
        return ((List) object).get(index);
      } catch (NumberFormatException nfe) {
        return null;
      }
    }

    @Override
    public boolean containsKey(Object key) {
      return get(key) != null;
    }

    @Override
    public Set<Entry<Object, Object>> entrySet() {
      Set<Entry<Object, Object>> retSet = new LinkedHashSet<>();
      Iterator<Object> it = this.iterator();
      int counter = 0;
      while (it.hasNext()) {
        Object val = it.next();
        Entry<Object, Object> e = new AbstractMap.SimpleEntry<>(counter++, val);
        retSet.add(e);
      }

      return retSet;
    }

    /**
     * Returns an iterator over a set of elements of type T.
     *
     * @return an Iterator.
     */
    @Override
    public Iterator<Object> iterator() {
      return ((List) object).iterator();
    }
  }

  //	public static void main(String[] args) throws Exception {
  //		Map<String, Object> taskOutput = new HashMap<>();
  //		taskOutput.put("headers", new String[] {"header1", "header2", "header3"});
  //		taskOutput.put("values", new ArrayList<Object[]>());
  //		((List<Object[]>) taskOutput.get("values")).add(new Object[] {"row11", 1, "row13"});
  //		((List<Object[]>) taskOutput.get("values")).add(new Object[] {"row21", 2, "row23"});
  //		((List<Object[]>) taskOutput.get("values")).add(new Object[] {"row23", 3, "row33"});
  //
  //		Map<String, Object> variables = new HashMap<>();
  //		variables.put("data", taskOutput);
  //
  //		String template = "<html>"
  //				+ "<table>\n" +
  //				"  <tr>\n" +
  //				"    <th>{{data.headers.0}}</th>\n" +
  //				"    <th>{{data.headers.1}}</th>\n" +
  //				"    <th>{{data.headers.2}}</th>\n" +
  //				"  </tr>\n" +
  //				"  <tr>\n" +
  //				"    <td>{{data.values.0.0}}</td>\n" +
  //				"    <td>{{data.values.0.1}}</td>\n" +
  //				"    <td>{{data.values.0.2}}</td>\n" +
  //				"  </tr>\n" +
  //				"  <tr>\n" +
  //				"    <td>{{data.values.1.0}}</td>\n" +
  //				"    <td>{{data.values.1.2}}</td>\n" +
  //				"    <td>{{data.values.1.2}}</td>\n" +
  //				"  </tr>\n" +
  //				"</table>\n</html>";
  //
  //		System.out.println("Example 1");
  //		System.out.println(MustacheUtility.compile(template, variables));
  //		System.out.println("");
  //		System.out.println("Example 2");
  //
  //		template = "<html>"
  //				+ "<table>\n" +
  //				"  <tr>\n" +
  //				"    <th>{{data.headers.0}}</th>\n" +
  //				"    <th>{{data.headers.1}}</th>\n" +
  //				"    <th>{{data.headers.2}}</th>\n" +
  //				"  </tr>\n"
  //				+ "{{#data.values}}"
  //				+ "  <tr>\n"
  //				+ "{{#.}}"
  //				+ "    <td>{{.}}</td>\n"
  //				+"{{/.}}"
  //				+ "\n  </tr>\n"
  //				+ "{{/data.values}}" +
  //				"</table>\n</html>";
  //
  //		System.out.println(MustacheUtility.compile(template, variables));
  //	}

}
