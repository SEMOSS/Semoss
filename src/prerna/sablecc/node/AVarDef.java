/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.Analysis;

@SuppressWarnings("nls")
public final class AVarDef extends PVarDef
{
    private TValprefix _valprefix_;
    private TId _valname_;

    public AVarDef()
    {
        // Constructor
    }

    public AVarDef(
        @SuppressWarnings("hiding") TValprefix _valprefix_,
        @SuppressWarnings("hiding") TId _valname_)
    {
        // Constructor
        setValprefix(_valprefix_);

        setValname(_valname_);

    }

    @Override
    public Object clone()
    {
        return new AVarDef(
            cloneNode(this._valprefix_),
            cloneNode(this._valname_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAVarDef(this);
    }

    public TValprefix getValprefix()
    {
        return this._valprefix_;
    }

    public void setValprefix(TValprefix node)
    {
        if(this._valprefix_ != null)
        {
            this._valprefix_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._valprefix_ = node;
    }

    public TId getValname()
    {
        return this._valname_;
    }

    public void setValname(TId node)
    {
        if(this._valname_ != null)
        {
            this._valname_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._valname_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._valprefix_)
            + toString(this._valname_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._valprefix_ == child)
        {
            this._valprefix_ = null;
            return;
        }

        if(this._valname_ == child)
        {
            this._valname_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._valprefix_ == oldChild)
        {
            setValprefix((TValprefix) newChild);
            return;
        }

        if(this._valname_ == oldChild)
        {
            setValname((TId) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
