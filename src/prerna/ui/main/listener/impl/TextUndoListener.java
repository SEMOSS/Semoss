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
package prerna.ui.main.listener.impl;

import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.KeyEvent;

import javax.swing.AbstractAction;
import javax.swing.ActionMap;
import javax.swing.InputMap;
import javax.swing.KeyStroke;
import javax.swing.event.UndoableEditEvent;
import javax.swing.event.UndoableEditListener;
import javax.swing.text.JTextComponent;
import javax.swing.undo.CannotRedoException;
import javax.swing.undo.CannotUndoException;
import javax.swing.undo.UndoManager;

/**
 * An Undo/Redo listener for use with a JTextComponent.
 *
 * <p> A convenience constructor has been provided such that a caller
 * can immediately have default shortcut key mappings for undo/redo
 * actions on the text component
 *
 * @author Ben Leinfelder
 * @version $Id: UndoListener.java 65763 2013-03-07 01:54:37Z cxh $
 * @since Ptolemy II 8.0


 * @author ben leinfelder
 */
public class TextUndoListener implements UndoableEditListener {

    /**
     * Construct an undo listener.
     * <p>This constructor allows simple usage without setting up key mapping
     * automatically (it is then the responsibilty of the caller to
     * provide a mechanism for invoking the undo and redo actions.)
     */
    public TextUndoListener() {
    }

    /**
     * Construct an undo listener with default key mappings.
     * The default key mappings invoke the undo and redo actions on
     * <i>textArea</i> .
     * <p>A typical usage pattern would be:
     * <code>
     *         JTextArea textArea = new JTextArea("testing");
     *         textArea.getDocument().addUndoableEditListener(new UndoListener(textArea));
     * </code>
     * @param textArea the text component that is being listened to
     * and upon which undo/redo actions will be performed
     */
    public TextUndoListener(JTextComponent textArea) {

        // Set the mapping for shortcut keys;
        InputMap inputMap = textArea.getInputMap();
        ActionMap actionMap = textArea.getActionMap();

        // Ctrl-z or equivalent to undo.
        inputMap.put(KeyStroke.getKeyStroke(KeyEvent.VK_Z, Toolkit
                .getDefaultToolkit().getMenuShortcutKeyMask()), "undo");
        actionMap.put("undo", _undoAction);
        // Ctrl-y or equivalent to redo
        inputMap.put(KeyStroke.getKeyStroke(KeyEvent.VK_Y, Toolkit
                .getDefaultToolkit().getMenuShortcutKeyMask()), "redo");
        actionMap.put("redo", _redoAction);
    }

    /** Remember the edit and update the action state.
     *  @param event The event that occurred.
     */
    public void undoableEditHappened(UndoableEditEvent event) {
        _undo.addEdit(event.getEdit());
        _undoAction._updateUndoState();
        _redoAction._updateRedoState();
    }

    /** The undo action. */
    protected UndoAction _undoAction = new UndoAction();

    /** The redo action. */
    protected RedoAction _redoAction = new RedoAction();

    /** The undo manager. */
    protected UndoManager _undo = new UndoManager();

    /**
     * Perform the undo action.
     * @author karverma
     * @version $Revision: 1.0 $
     */
    protected class UndoAction extends AbstractAction {
        /**
         * Constructor for UndoAction.
         */
        public UndoAction() {
            super("Undo");
            setEnabled(false);
        }

        /**
         * Method actionPerformed.
         * @param e ActionEvent
         */
        public void actionPerformed(ActionEvent e) {
            try {
                _undo.undo();
            } catch (CannotUndoException ex) {
                throw new RuntimeException("Unable to undo.", ex);
            }
            _updateUndoState();
            _redoAction._updateRedoState();
        }

        /** Depending on the whether the undo manager can undo, enable
         *  and disable the undo state.
         */
        protected void _updateUndoState() {
            if (_undo.canUndo()) {
                setEnabled(true);
            } else {
                setEnabled(false);
            }
        }
    }

    /**
     * Peform the redo action.
     * @author karverma
     * @version $Revision: 1.0 $
     */
    protected class RedoAction extends AbstractAction {
        /**
         * Constructor for RedoAction.
         */
        public RedoAction() {
            super("Redo");
            setEnabled(false);
        }

        /**
         * Method actionPerformed.
         * @param e ActionEvent
         */
        public void actionPerformed(ActionEvent e) {
            try {
                _undo.redo();
            } catch (CannotRedoException ex) {
                throw new RuntimeException("Unable to redo.", ex);
            }
            _updateRedoState();
            _undoAction._updateUndoState();
        }

        /** Depending on the whether the undo manager can redo, enable
         *  and disable the undo state.
         */
        protected void _updateRedoState() {
            if (_undo.canRedo()) {
                setEnabled(true);
            } else {
                setEnabled(false);
            }
        }
    }
}
