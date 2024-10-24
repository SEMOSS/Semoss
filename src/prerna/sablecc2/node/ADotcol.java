/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.*;

@SuppressWarnings("nls")
public final class ADotcol extends PDotcol
{
    private TFrameid _frameid_;
    private TId _columnName_;

    public ADotcol()
    {
        // Constructor
    }

    public ADotcol(
        @SuppressWarnings("hiding") TFrameid _frameid_,
        @SuppressWarnings("hiding") TId _columnName_)
    {
        // Constructor
        setFrameid(_frameid_);

        setColumnName(_columnName_);

    }

    @Override
    public Object clone()
    {
        return new ADotcol(
            cloneNode(this._frameid_),
            cloneNode(this._columnName_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseADotcol(this);
    }

    public TFrameid getFrameid()
    {
        return this._frameid_;
    }

    public void setFrameid(TFrameid node)
    {
        if(this._frameid_ != null)
        {
            this._frameid_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._frameid_ = node;
    }

    public TId getColumnName()
    {
        return this._columnName_;
    }

    public void setColumnName(TId node)
    {
        if(this._columnName_ != null)
        {
            this._columnName_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._columnName_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._frameid_)
            + toString(this._columnName_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._frameid_ == child)
        {
            this._frameid_ = null;
            return;
        }

        if(this._columnName_ == child)
        {
            this._columnName_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._frameid_ == oldChild)
        {
            setFrameid((TFrameid) newChild);
            return;
        }

        if(this._columnName_ == oldChild)
        {
            setColumnName((TId) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
