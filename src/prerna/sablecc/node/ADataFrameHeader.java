/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class ADataFrameHeader extends PDataFrameHeader
{
    private TDataframeheader _dataframeheader_;

    public ADataFrameHeader()
    {
        // Constructor
    }

    public ADataFrameHeader(
        @SuppressWarnings("hiding") TDataframeheader _dataframeheader_)
    {
        // Constructor
        setDataframeheader(_dataframeheader_);

    }

    @Override
    public Object clone()
    {
        return new ADataFrameHeader(
            cloneNode(this._dataframeheader_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseADataFrameHeader(this);
    }

    public TDataframeheader getDataframeheader()
    {
        return this._dataframeheader_;
    }

    public void setDataframeheader(TDataframeheader node)
    {
        if(this._dataframeheader_ != null)
        {
            this._dataframeheader_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._dataframeheader_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._dataframeheader_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._dataframeheader_ == child)
        {
            this._dataframeheader_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._dataframeheader_ == oldChild)
        {
            setDataframeheader((TDataframeheader) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
