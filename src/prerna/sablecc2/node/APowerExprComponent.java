/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.*;

@SuppressWarnings("nls")
public final class APowerExprComponent extends PExprComponent
{
    private PPower _power_;

    public APowerExprComponent()
    {
        // Constructor
    }

    public APowerExprComponent(
        @SuppressWarnings("hiding") PPower _power_)
    {
        // Constructor
        setPower(_power_);

    }

    @Override
    public Object clone()
    {
        return new APowerExprComponent(
            cloneNode(this._power_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAPowerExprComponent(this);
    }

    public PPower getPower()
    {
        return this._power_;
    }

    public void setPower(PPower node)
    {
        if(this._power_ != null)
        {
            this._power_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._power_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._power_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._power_ == child)
        {
            this._power_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._power_ == oldChild)
        {
            setPower((PPower) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
