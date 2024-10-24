/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.*;

@SuppressWarnings("nls")
public final class ABasicOrComparisonTerm extends PComparisonTerm
{
    private POrComparison _orComparison_;

    public ABasicOrComparisonTerm()
    {
        // Constructor
    }

    public ABasicOrComparisonTerm(
        @SuppressWarnings("hiding") POrComparison _orComparison_)
    {
        // Constructor
        setOrComparison(_orComparison_);

    }

    @Override
    public Object clone()
    {
        return new ABasicOrComparisonTerm(
            cloneNode(this._orComparison_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseABasicOrComparisonTerm(this);
    }

    public POrComparison getOrComparison()
    {
        return this._orComparison_;
    }

    public void setOrComparison(POrComparison node)
    {
        if(this._orComparison_ != null)
        {
            this._orComparison_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._orComparison_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._orComparison_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._orComparison_ == child)
        {
            this._orComparison_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._orComparison_ == oldChild)
        {
            setOrComparison((POrComparison) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
