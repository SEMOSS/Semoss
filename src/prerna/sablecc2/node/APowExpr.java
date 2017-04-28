/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.*;

@SuppressWarnings("nls")
public final class APowExpr extends PExpr
{
    private PTerm _left_;
    private TPow _pow_;
    private PExpr _right_;

    public APowExpr()
    {
        // Constructor
    }

    public APowExpr(
        @SuppressWarnings("hiding") PTerm _left_,
        @SuppressWarnings("hiding") TPow _pow_,
        @SuppressWarnings("hiding") PExpr _right_)
    {
        // Constructor
        setLeft(_left_);

        setPow(_pow_);

        setRight(_right_);

    }

    @Override
    public Object clone()
    {
        return new APowExpr(
            cloneNode(this._left_),
            cloneNode(this._pow_),
            cloneNode(this._right_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAPowExpr(this);
    }

    public PTerm getLeft()
    {
        return this._left_;
    }

    public void setLeft(PTerm node)
    {
        if(this._left_ != null)
        {
            this._left_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._left_ = node;
    }

    public TPow getPow()
    {
        return this._pow_;
    }

    public void setPow(TPow node)
    {
        if(this._pow_ != null)
        {
            this._pow_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._pow_ = node;
    }

    public PExpr getRight()
    {
        return this._right_;
    }

    public void setRight(PExpr node)
    {
        if(this._right_ != null)
        {
            this._right_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._right_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._left_)
            + toString(this._pow_)
            + toString(this._right_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._left_ == child)
        {
            this._left_ = null;
            return;
        }

        if(this._pow_ == child)
        {
            this._pow_ = null;
            return;
        }

        if(this._right_ == child)
        {
            this._right_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._left_ == oldChild)
        {
            setLeft((PTerm) newChild);
            return;
        }

        if(this._pow_ == oldChild)
        {
            setPow((TPow) newChild);
            return;
        }

        if(this._right_ == oldChild)
        {
            setRight((PExpr) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
