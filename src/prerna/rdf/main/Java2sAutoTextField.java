/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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
package prerna.rdf.main;

import java.util.List;

import javax.swing.JTextField;
import javax.swing.text.AttributeSet;
import javax.swing.text.BadLocationException;
import javax.swing.text.PlainDocument;

/**
 */
public class Java2sAutoTextField extends JTextField {
	
  /**
   */
  class AutoDocument extends PlainDocument {

    /**
     * Method replace.
     * @param i int
     * @param j int
     * @param s String
     * @param attributeset AttributeSet
     */
    public void replace(int i, int j, String s, AttributeSet attributeset)
        throws BadLocationException {
      super.remove(i, j);
      insertString(i, s, attributeset);
    }

    /**
     * Method insertString.
     * @param i int
     * @param s String
     * @param attributeset AttributeSet
     */
    public void insertString(int i, String s, AttributeSet attributeset)
        throws BadLocationException {
      if (s == null || "".equals(s))
        return;
      String s1 = getText(0, i);
      String s2 = getMatch(s1 + s);
      int j = (i + s.length()) - 1;
      if (isStrict && s2 == null) {
        s2 = getMatch(s1);
        j--;
      } else if (!isStrict && s2 == null) {
        super.insertString(i, s, attributeset);
        return;
      }
      if (autoComboBox != null && s2 != null)
        autoComboBox.setSelectedValue(s2);
      super.remove(0, getLength());
      super.insertString(0, s2, attributeset);
      setSelectionStart(j + 1);
      setSelectionEnd(getLength());
    }

    /**
     * Method remove.
     * @param i int
     * @param j int
     */
    public void remove(int i, int j) throws BadLocationException {
      int k = getSelectionStart();
      if (k > 0)
        k--;
      String s = getMatch(getText(0, k));
      if (!isStrict && s == null) {
        super.remove(i, j);
      } else {
        super.remove(0, getLength());
        super.insertString(0, s, null);
      }
      if (autoComboBox != null && s != null)
        autoComboBox.setSelectedValue(s);
      try {
        setSelectionStart(k);
        setSelectionEnd(getLength());
      } catch (Exception exception) {
      }
    }

  }

  /**
   * Constructor for Java2sAutoTextField.
   * @param list List
   */
  public Java2sAutoTextField(List list) {
    isCaseSensitive = false;
    isStrict = true;
    autoComboBox = null;
    if (list == null) {
      throw new IllegalArgumentException("values can not be null");
    } else {
      dataList = list;
      init();
      return;
    }
  }

  /**
   * Constructor for Java2sAutoTextField.
   * @param list List
   * @param b Java2sAutoComboBox
   */
  Java2sAutoTextField(List list, Java2sAutoComboBox b) {
    isCaseSensitive = false;
    isStrict = true;
    autoComboBox = null;
    if (list == null) {
      throw new IllegalArgumentException("values can not be null");
    } else {
      dataList = list;
      autoComboBox = b;
      init();
      return;
    }
  }

  /**
   * Method init.
   */
  private void init() {
    setDocument(new AutoDocument());
    if (isStrict && dataList.size() > 0)
      setText(dataList.get(0).toString());
  }

  /**
   * Method getMatch.
   * @param s String
  
   * @return String */
  private String getMatch(String s) {
    for (int i = 0; i < dataList.size(); i++) {
      String s1 = dataList.get(i).toString();
      if (s1 != null) {
        if (!isCaseSensitive
            && s1.toLowerCase().startsWith(s.toLowerCase()))
          return s1;
        if (isCaseSensitive && s1.startsWith(s))
          return s1;
      }
    }

    return null;
  }

  /**
   * Method replaceSelection.
   * @param s String
   */
  public void replaceSelection(String s) {
    AutoDocument _lb = (AutoDocument) getDocument();
    if (_lb != null)
      try {
        int i = Math.min(getCaret().getDot(), getCaret().getMark());
        int j = Math.max(getCaret().getDot(), getCaret().getMark());
        _lb.replace(i, j - i, s, null);
      } catch (Exception exception) {
      }
  }

  /**
   * Method isCaseSensitive.
  
   * @return boolean */
  public boolean isCaseSensitive() {
    return isCaseSensitive;
  }

  /**
   * Method setCaseSensitive.
   * @param flag boolean
   */
  public void setCaseSensitive(boolean flag) {
    isCaseSensitive = flag;
  }

  /**
   * Method isStrict.
  
   * @return boolean */
  public boolean isStrict() {
    return isStrict;
  }

  /**
   * Method setStrict.
   * @param flag boolean
   */
  public void setStrict(boolean flag) {
    isStrict = flag;
  }

  /**
   * Method getDataList.
  
   * @return List */
  public List getDataList() {
    return dataList;
  }

  /**
   * Method setDataList.
   * @param list List
   */
  public void setDataList(List list) {
    if (list == null) {
      throw new IllegalArgumentException("values can not be null");
    } else {
      dataList = list;
      return;
    }
  }

  private List dataList;

  private boolean isCaseSensitive;

  private boolean isStrict;

  private Java2sAutoComboBox autoComboBox;
}

