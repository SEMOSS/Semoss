/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.rdf.main;

import java.awt.event.ItemEvent;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.plaf.basic.BasicComboBoxEditor;

/**
 */
public class Java2sAutoComboBox extends JComboBox {
  /**
   */
  private class AutoTextFieldEditor extends BasicComboBoxEditor {

    /**
     * Method getAutoTextFieldEditor.
    
     * @return Java2sAutoTextField */
    private Java2sAutoTextField getAutoTextFieldEditor() {
      return (Java2sAutoTextField) editor;
    }

    /**
     * Constructor for AutoTextFieldEditor.
     * @param list java.util.List
     */
    AutoTextFieldEditor(java.util.List list) {
      editor = new Java2sAutoTextField(list, Java2sAutoComboBox.this);
    }
  }

  /**
   * Constructor for Java2sAutoComboBox.
   * @param list java.util.List
   */
  public Java2sAutoComboBox(java.util.List list) {
    isFired = false;
    autoTextFieldEditor = new AutoTextFieldEditor(list);
    setEditable(true);
    setModel(new DefaultComboBoxModel(list.toArray()) {

      protected void fireContentsChanged(Object obj, int i, int j) {
        if (!isFired)
          super.fireContentsChanged(obj, i, j);
      }

    });
    setEditor(autoTextFieldEditor);
  }

  /**
   * Method isCaseSensitive.
  
   * @return boolean */
  public boolean isCaseSensitive() {
    return autoTextFieldEditor.getAutoTextFieldEditor().isCaseSensitive();
  }

  /**
   * Method setCaseSensitive.
   * @param flag boolean
   */
  public void setCaseSensitive(boolean flag) {
    autoTextFieldEditor.getAutoTextFieldEditor().setCaseSensitive(flag);
  }

  /**
   * Method isStrict.
  
   * @return boolean */
  public boolean isStrict() {
    return autoTextFieldEditor.getAutoTextFieldEditor().isStrict();
  }

  /**
   * Method setStrict.
   * @param flag boolean
   */
  public void setStrict(boolean flag) {
    autoTextFieldEditor.getAutoTextFieldEditor().setStrict(flag);
  }

  /**
   * Method getDataList.
  
   * @return java.util.List */
  public java.util.List getDataList() {
    return autoTextFieldEditor.getAutoTextFieldEditor().getDataList();
  }

  /**
   * Method setDataList.
   * @param list java.util.List
   */
  public void setDataList(java.util.List list) {
    autoTextFieldEditor.getAutoTextFieldEditor().setDataList(list);
    setModel(new DefaultComboBoxModel(list.toArray()));
  }

  /**
   * Method setSelectedValue.
   * @param obj Object
   */
  void setSelectedValue(Object obj) {
    if (isFired) {
      return;
    } else {
      isFired = true;
      setSelectedItem(obj);
      fireItemStateChanged(new ItemEvent(this, 701, selectedItemReminder,
          1));
      isFired = false;
      return;
    }
  }

  /**
   * Method fireActionEvent.
   */
  protected void fireActionEvent() {
    if (!isFired)
      super.fireActionEvent();
  }

  private AutoTextFieldEditor autoTextFieldEditor;

  private boolean isFired;

}
