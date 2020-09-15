/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.*;

@SuppressWarnings("nls")
public final class ASimpleSubRoutineOptions extends PSubRoutineOptions
{
    private PBaseSubExpr _baseSubExpr_;

    public ASimpleSubRoutineOptions()
    {
        // Constructor
    }

    public ASimpleSubRoutineOptions(
        @SuppressWarnings("hiding") PBaseSubExpr _baseSubExpr_)
    {
        // Constructor
        setBaseSubExpr(_baseSubExpr_);

    }

    @Override
    public Object clone()
    {
        return new ASimpleSubRoutineOptions(
            cloneNode(this._baseSubExpr_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseASimpleSubRoutineOptions(this);
    }

    public PBaseSubExpr getBaseSubExpr()
    {
        return this._baseSubExpr_;
    }

    public void setBaseSubExpr(PBaseSubExpr node)
    {
        if(this._baseSubExpr_ != null)
        {
            this._baseSubExpr_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._baseSubExpr_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._baseSubExpr_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._baseSubExpr_ == child)
        {
            this._baseSubExpr_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._baseSubExpr_ == oldChild)
        {
            setBaseSubExpr((PBaseSubExpr) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
