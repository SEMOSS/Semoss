/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.*;

@SuppressWarnings("nls")
public final class AInputOpInput extends POpInput
{
    private PColDef _colDef_;

    public AInputOpInput()
    {
        // Constructor
    }

    public AInputOpInput(
        @SuppressWarnings("hiding") PColDef _colDef_)
    {
        // Constructor
        setColDef(_colDef_);

    }

    @Override
    public Object clone()
    {
        return new AInputOpInput(
            cloneNode(this._colDef_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAInputOpInput(this);
    }

    public PColDef getColDef()
    {
        return this._colDef_;
    }

    public void setColDef(PColDef node)
    {
        if(this._colDef_ != null)
        {
            this._colDef_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._colDef_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._colDef_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._colDef_ == child)
        {
            this._colDef_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._colDef_ == oldChild)
        {
            setColDef((PColDef) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
