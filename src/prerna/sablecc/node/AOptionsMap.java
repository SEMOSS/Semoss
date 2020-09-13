/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.Analysis;

@SuppressWarnings("nls")
public final class AOptionsMap extends POptionsMap
{
    private TComma _comma_;
    private PMapObj _mapObj_;

    public AOptionsMap()
    {
        // Constructor
    }

    public AOptionsMap(
        @SuppressWarnings("hiding") TComma _comma_,
        @SuppressWarnings("hiding") PMapObj _mapObj_)
    {
        // Constructor
        setComma(_comma_);

        setMapObj(_mapObj_);

    }

    @Override
    public Object clone()
    {
        return new AOptionsMap(
            cloneNode(this._comma_),
            cloneNode(this._mapObj_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAOptionsMap(this);
    }

    public TComma getComma()
    {
        return this._comma_;
    }

    public void setComma(TComma node)
    {
        if(this._comma_ != null)
        {
            this._comma_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._comma_ = node;
    }

    public PMapObj getMapObj()
    {
        return this._mapObj_;
    }

    public void setMapObj(PMapObj node)
    {
        if(this._mapObj_ != null)
        {
            this._mapObj_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._mapObj_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._comma_)
            + toString(this._mapObj_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._comma_ == child)
        {
            this._comma_ = null;
            return;
        }

        if(this._mapObj_ == child)
        {
            this._mapObj_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._comma_ == oldChild)
        {
            setComma((TComma) newChild);
            return;
        }

        if(this._mapObj_ == oldChild)
        {
            setMapObj((PMapObj) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
