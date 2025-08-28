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
package prerna.engine.impl;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Set;

/**
 * A Properties implementation that treats string keys in a case-insensitive manner. All string keys
 * are normalized to uppercase internally for consistent lookup.
 */
public class CaseInsensitiveProperties extends Properties implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Creates an empty case-insensitive properties object. */
  public CaseInsensitiveProperties() {
    super();
  }

  /**
   * Creates a case-insensitive properties object with the same mappings as the given Properties.
   * All string keys from the source properties are normalized to uppercase.
   *
   * @param properties the Properties to copy from
   */
  public CaseInsensitiveProperties(Properties properties) {
    super();
    if (properties != null) {
      // Synchronize on the source properties to ensure thread safety during iteration
      synchronized (properties) {
        for (Object key : properties.keySet()) {
          Object value = properties.get(key);
          // Use our put method to ensure proper key normalization
          put(key, value);
        }
      }
    }
  }

  /**
   * Normalizes string keys to uppercase for case-insensitive comparison.
   *
   * @param key the key to normalize
   * @return the normalized key (uppercase if string), or the original key if not a string
   */
  private Object normalizeKey(Object key) {
    return (key instanceof String) ? ((String) key).toUpperCase() : key;
  }

  @Override
  public synchronized Object setProperty(String key, String value) {
    return super.setProperty(key.toUpperCase(), value);
  }

  @Override
  public synchronized Object put(Object key, Object value) {
    return super.put(normalizeKey(key), value);
  }

  @Override
  public synchronized Object putIfAbsent(Object key, Object value) {
    return super.putIfAbsent(normalizeKey(key), value);
  }

  @Override
  public String getProperty(String key) {
    return super.getProperty(key.toUpperCase());
  }

  @Override
  public String getProperty(String key, String defaultValue) {
    return super.getProperty(key.toUpperCase(), defaultValue);
  }

  @Override
  public synchronized Object get(Object key) {
    return super.get(normalizeKey(key));
  }

  @Override
  public synchronized Object getOrDefault(Object key, Object defaultValue) {
    return super.getOrDefault(normalizeKey(key), defaultValue);
  }

  @Override
  public synchronized boolean containsKey(Object key) {
    return super.containsKey(normalizeKey(key));
  }

  @Override
  public synchronized Object remove(Object key) {
    return super.remove(normalizeKey(key));
  }

  @Override
  public synchronized boolean remove(Object key, Object value) {
    return super.remove(normalizeKey(key), value);
  }

  @Override
  public synchronized Object replace(Object key, Object value) {
    return super.replace(normalizeKey(key), value);
  }

  @Override
  public synchronized boolean replace(Object key, Object oldValue, Object newValue) {
    return super.replace(normalizeKey(key), oldValue, newValue);
  }

  /**
   * Returns an enumeration of the property names, preserving the normalized (uppercase) form. Note:
   * The returned keys will be in uppercase form as they are stored internally.
   */
  @Override
  public Enumeration<?> propertyNames() {
    return super.propertyNames();
  }

  /**
   * Returns a set of keys, preserving the normalized (uppercase) form. Note: The returned keys will
   * be in uppercase form as they are stored internally.
   */
  @Override
  public synchronized Set<Object> keySet() {
    return super.keySet();
  }
}
