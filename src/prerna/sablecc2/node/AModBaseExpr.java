/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.*;

@SuppressWarnings("nls")
public final class AModBaseExpr extends PBaseExpr
{
    private PExprComponent _left_;
    private TMod _mod_;
    private PBaseExpr _right_;

    public AModBaseExpr()
    {
        // Constructor
    }

    public AModBaseExpr(
        @SuppressWarnings("hiding") PExprComponent _left_,
        @SuppressWarnings("hiding") TMod _mod_,
        @SuppressWarnings("hiding") PBaseExpr _right_)
    {
        // Constructor
        setLeft(_left_);

        setMod(_mod_);

        setRight(_right_);

    }

    @Override
    public Object clone()
    {
        return new AModBaseExpr(
            cloneNode(this._left_),
            cloneNode(this._mod_),
            cloneNode(this._right_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAModBaseExpr(this);
    }

    public PExprComponent getLeft()
    {
        return this._left_;
    }

    public void setLeft(PExprComponent node)
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

    public TMod getMod()
    {
        return this._mod_;
    }

    public void setMod(TMod node)
    {
        if(this._mod_ != null)
        {
            this._mod_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._mod_ = node;
    }

    public PBaseExpr getRight()
    {
        return this._right_;
    }

    public void setRight(PBaseExpr node)
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
            + toString(this._mod_)
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

        if(this._mod_ == child)
        {
            this._mod_ = null;
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
            setLeft((PExprComponent) newChild);
            return;
        }

        if(this._mod_ == oldChild)
        {
            setMod((TMod) newChild);
            return;
        }

        if(this._right_ == oldChild)
        {
            setRight((PBaseExpr) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
