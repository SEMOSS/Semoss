/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.*;

@SuppressWarnings("nls")
public final class AListMapExtendedInput extends PMapExtendedInput
{
    private PMapList _mapList_;

    public AListMapExtendedInput()
    {
        // Constructor
    }

    public AListMapExtendedInput(
        @SuppressWarnings("hiding") PMapList _mapList_)
    {
        // Constructor
        setMapList(_mapList_);

    }

    @Override
    public Object clone()
    {
        return new AListMapExtendedInput(
            cloneNode(this._mapList_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAListMapExtendedInput(this);
    }

    public PMapList getMapList()
    {
        return this._mapList_;
    }

    public void setMapList(PMapList node)
    {
        if(this._mapList_ != null)
        {
            this._mapList_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._mapList_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._mapList_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._mapList_ == child)
        {
            this._mapList_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._mapList_ == oldChild)
        {
            setMapList((PMapList) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
