/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.Analysis;

@SuppressWarnings("nls")
public final class AComparisonGroupComparisonExpr extends PComparisonExpr
{
    private PComparisonGroup _comparisonGroup_;

    public AComparisonGroupComparisonExpr()
    {
        // Constructor
    }

    public AComparisonGroupComparisonExpr(
        @SuppressWarnings("hiding") PComparisonGroup _comparisonGroup_)
    {
        // Constructor
        setComparisonGroup(_comparisonGroup_);

    }

    @Override
    public Object clone()
    {
        return new AComparisonGroupComparisonExpr(
            cloneNode(this._comparisonGroup_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAComparisonGroupComparisonExpr(this);
    }

    public PComparisonGroup getComparisonGroup()
    {
        return this._comparisonGroup_;
    }

    public void setComparisonGroup(PComparisonGroup node)
    {
        if(this._comparisonGroup_ != null)
        {
            this._comparisonGroup_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._comparisonGroup_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._comparisonGroup_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._comparisonGroup_ == child)
        {
            this._comparisonGroup_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._comparisonGroup_ == oldChild)
        {
            setComparisonGroup((PComparisonGroup) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
