/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class AExprWordOrNum extends PWordOrNum
{
    private PFormula _formula_;

    public AExprWordOrNum()
    {
        // Constructor
    }

    public AExprWordOrNum(
        @SuppressWarnings("hiding") PFormula _formula_)
    {
        // Constructor
        setFormula(_formula_);

    }

    @Override
    public Object clone()
    {
        return new AExprWordOrNum(
            cloneNode(this._formula_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAExprWordOrNum(this);
    }

    public PFormula getFormula()
    {
        return this._formula_;
    }

    public void setFormula(PFormula node)
    {
        if(this._formula_ != null)
        {
            this._formula_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._formula_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._formula_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._formula_ == child)
        {
            this._formula_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._formula_ == oldChild)
        {
            setFormula((PFormula) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
