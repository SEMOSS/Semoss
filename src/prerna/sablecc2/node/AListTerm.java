/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.*;

@SuppressWarnings("nls")
public final class AListTerm extends PTerm
{
    private PList _list_;

    public AListTerm()
    {
        // Constructor
    }

    public AListTerm(
        @SuppressWarnings("hiding") PList _list_)
    {
        // Constructor
        setList(_list_);

    }

    @Override
    public Object clone()
    {
        return new AListTerm(
            cloneNode(this._list_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAListTerm(this);
    }

    public PList getList()
    {
        return this._list_;
    }

    public void setList(PList node)
    {
        if(this._list_ != null)
        {
            this._list_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._list_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._list_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._list_ == child)
        {
            this._list_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._list_ == oldChild)
        {
            setList((PList) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
