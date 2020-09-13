/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.Analysis;

@SuppressWarnings("nls")
public final class AMapEntry extends PMapEntry
{
    private PMapKey _key_;
    private TColon _colon_;
    private PValues _val_;

    public AMapEntry()
    {
        // Constructor
    }

    public AMapEntry(
        @SuppressWarnings("hiding") PMapKey _key_,
        @SuppressWarnings("hiding") TColon _colon_,
        @SuppressWarnings("hiding") PValues _val_)
    {
        // Constructor
        setKey(_key_);

        setColon(_colon_);

        setVal(_val_);

    }

    @Override
    public Object clone()
    {
        return new AMapEntry(
            cloneNode(this._key_),
            cloneNode(this._colon_),
            cloneNode(this._val_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAMapEntry(this);
    }

    public PMapKey getKey()
    {
        return this._key_;
    }

    public void setKey(PMapKey node)
    {
        if(this._key_ != null)
        {
            this._key_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._key_ = node;
    }

    public TColon getColon()
    {
        return this._colon_;
    }

    public void setColon(TColon node)
    {
        if(this._colon_ != null)
        {
            this._colon_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._colon_ = node;
    }

    public PValues getVal()
    {
        return this._val_;
    }

    public void setVal(PValues node)
    {
        if(this._val_ != null)
        {
            this._val_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._val_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._key_)
            + toString(this._colon_)
            + toString(this._val_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._key_ == child)
        {
            this._key_ = null;
            return;
        }

        if(this._colon_ == child)
        {
            this._colon_ = null;
            return;
        }

        if(this._val_ == child)
        {
            this._val_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._key_ == oldChild)
        {
            setKey((PMapKey) newChild);
            return;
        }

        if(this._colon_ == oldChild)
        {
            setColon((TColon) newChild);
            return;
        }

        if(this._val_ == oldChild)
        {
            setVal((PValues) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
