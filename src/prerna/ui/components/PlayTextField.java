/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.components;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.lang.reflect.Field;

import javax.swing.ComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JList;
import javax.swing.ListCellRenderer;
import javax.swing.plaf.basic.BasicComboBoxUI;
import javax.swing.text.AttributeSet;
import javax.swing.text.BadLocationException;
import javax.swing.text.JTextComponent;
import javax.swing.text.PlainDocument;


/**
 * This is used to extend the functionalities of the text field in the combo box.
 */
public class PlayTextField extends PlainDocument{
    JComboBox comboBox;
    ComboBoxModel model;
    JTextComponent editor;
    // flag to indicate if setSelectedItem has been called
    // subsequent calls to remove/insertString should be ignored
    boolean selecting=false;
    boolean hidePopupOnFocusLoss;
    boolean hitBackspace=false;
    boolean hitBackspaceOnSelection;
    
    /**
     * Constructor for PlayTextField.
     * @param comboBox JComboBox
     */
    public PlayTextField(final JComboBox comboBox) {
        this.comboBox = comboBox;
        
        model = comboBox.getModel();
        editor = (JTextComponent) comboBox.getEditor().getEditorComponent();
        editor.setDocument(this);
        comboBox.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                if (!selecting) highlightCompletedText(0);
            }
        });
        editor.addKeyListener(new KeyAdapter() {
            public void keyPressed(KeyEvent e) {
                if (comboBox.isDisplayable()) comboBox.setPopupVisible(true);
                hitBackspace=false;
                switch (e.getKeyCode()) {
                    // determine if the pressed key is backspace (needed by the remove method)
                    case KeyEvent.VK_BACK_SPACE : hitBackspace=true;
                                                  hitBackspaceOnSelection=editor.getSelectionStart()!=editor.getSelectionEnd();
                                                  break;
                    // ignore delete key
                    case KeyEvent.VK_DELETE : e.consume();
                                              comboBox.getToolkit().beep();
                                              break;
                }
            }
        });
        // Bug 5100422 on Java 1.5: Editable JComboBox won't hide popup when tabbing out
        hidePopupOnFocusLoss=System.getProperty("java.version").startsWith("1.5");
        // Highlight whole text when gaining focus
        editor.addFocusListener(new FocusAdapter() {
            public void focusGained(FocusEvent e) {
                highlightCompletedText(0);
            }
            public void focusLost(FocusEvent e) {
                // Workaround for Bug 5100422 - Hide Popup on focus loss
                if (hidePopupOnFocusLoss) comboBox.setPopupVisible(false);
            }
        });
        setPrototypeValue();
        // Handle initially selected object
        Object selected = comboBox.getSelectedItem();
        if (selected!=null) setText(selected.toString());
        highlightCompletedText(0);
    }
    
    /**
     * Sets the prototype value.
     */
    public void setPrototypeValue() {
        JList list = getListBox();
        setPrototypeValue(getPrototypeValue(list), list);
    }
    
    /**
     * Sets the prototype value.
     * @param value Object
     * @param list JList
     */
    void setPrototypeValue(Object value, JList list) {
        comboBox.setPrototypeDisplayValue(value);
        list.setPrototypeCellValue(value);
    }
    
    /**
     * Gets the prototype value.
     * Returns the renderer used to display the selected item in the JComboBox field.
     * Iterates through the model and gets the list cell renderer components to get prototype values.
     * 
     * @param list 			JList used to get the list cell renderer component.
    
     * @return Object		Prototype value. */
    Object getPrototypeValue(JList list) {
        Object prototypeValue=null;
        double prototypeWidth=0;
        ListCellRenderer renderer = comboBox.getRenderer();
        for (int i=0, n=model.getSize(); i<n; i++) {
            Object value = model.getElementAt(i);
            java.awt.Component c = renderer.getListCellRendererComponent(list, value, i, false, false);
            double width = c.getPreferredSize().getWidth();
            if (width>prototypeWidth) {
                prototypeWidth=width;
                prototypeValue=value;
            }
        }
        return prototypeValue;
    }
    
    /**
     * Get a JList that displays a list of objects and allows the user to select an item.
     * Gets the fields that refer to the UI and the list box.
    
     * @return JList 	List of fields. */
    JList getListBox() {
        JList listBox;
        try {
            Field field = JComponent.class.getDeclaredField("ui");
            field.setAccessible(true);
            BasicComboBoxUI ui = (BasicComboBoxUI) field.get(comboBox);
            field = BasicComboBoxUI.class.getDeclaredField("listBox");
            field.setAccessible(true);
            listBox = (JList) field.get(ui);
        } catch (NoSuchFieldException nsfe) {
            throw new RuntimeException(nsfe);
        } catch (IllegalAccessException iae) {
            throw new RuntimeException(iae);
        }
        return listBox;
    }
    
    /**
     * Removes some content in the document when the user hits backspace.
     * @param offs 		The starting offset - must be >= 0.
     * @param len 		The number of characters to remove - must be >= 0.
     */
    public void remove(int offs, int len) throws BadLocationException {
        // return immediately when selecting an item
        if (selecting) return;
        if (hitBackspace) {
            // user hit backspace => move the selection backwards
            // old item keeps being selected
            if (offs>0) {
                if (hitBackspaceOnSelection) offs--;
            } else {
                // User hit backspace with the cursor positioned on the start => beep
                comboBox.getToolkit().beep(); // when available use: UIManager.getLookAndFeel().provideErrorFeedback(comboBox);
            }
            highlightCompletedText(offs);
        } else {
            super.remove(offs, len);
        }
    }
    
    /**
     * Inserts some content into the document.
     * @param offs 		The starting offset - must be >= 0.
     * @param str 		The string to be inserted.
     * @param a 		The attribute set for the inserted content.
     */
    public void insertString(int offs, String str, AttributeSet a) throws BadLocationException {
        // return immediately when selecting an item
        if (selecting) return;
        // insert the string into the document
        super.insertString(offs, str, a);
        // lookup and select a matching item
        Object item = lookupItem(getText(0, getLength()));
        if (item != null) {
            setSelectedItem(item);
        } else {
            // keep old item selected if there is no match
            item = comboBox.getSelectedItem();
            // imitate no insert (later on offs will be incremented by str.length(): selection won't move forward)
            offs = offs-str.length();
            // provide feedback to the user that his input has been received but can not be accepted
            comboBox.getToolkit().beep(); // when available use: UIManager.getLookAndFeel().provideErrorFeedback(comboBox);
        }
        setText(item.toString());
        // select the completed part
        highlightCompletedText(offs+str.length());
    }
    
    /**
     * Removes all text and inserts the completed string.
     * @param text 	Text to be set.
     */
    private void setText(String text) {
        try {
            super.remove(0, getLength());
            super.insertString(0, text, null);
        } catch (BadLocationException e) {
            throw new RuntimeException(e.toString());
        }
    }
    
    /**
     * Highlights completed text.
     * @param start 	Starting position of the caret.
     */
    private void highlightCompletedText(int start) {
        editor.setCaretPosition(getLength());
        editor.moveCaretPosition(start);
    }
    
    /**
     * Sets selected item in the model.
     * @param item 	Item to be set.
     */
    private void setSelectedItem(Object item) {
        selecting = true;
        model.setSelectedItem(item);
        selecting = false;
    }
    
    /**
     * Iterates over items in a model and sees if the text pattern already exists.
     * @param pattern 	Pattern to search for.
    
     * @return Object	Item that starts with a specific pattern. */
    private Object lookupItem(String pattern) {
        Object selectedItem = model.getSelectedItem();
        // only search for a different item if the currently selected does not match
        if (selectedItem != null && startsWithIgnoreCase(selectedItem.toString(), pattern)) {
            return selectedItem;
        } else {
            // iterate over all items
            for (int i=0, n=model.getSize(); i < n; i++) {
                Object currentItem = model.getElementAt(i);
                // current item starts with the pattern?
                if (startsWithIgnoreCase(currentItem.toString(), pattern)) return currentItem;
            }
        }
        // no item starts with the pattern => return null
        return null;
    }
    
    /**
     * Check if string 1 starts with string 2, ignoring case.
     * @param str1 String 1.
     * @param str2 String 2.
    
     * @return boolean	True if string 1 starts with string 2. */
    private boolean startsWithIgnoreCase(String str1, String str2) {
        return str1.toUpperCase().startsWith(str2.toUpperCase());
    }    

}
