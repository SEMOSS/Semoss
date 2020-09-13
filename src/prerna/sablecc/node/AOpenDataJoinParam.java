/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.Analysis;

@SuppressWarnings("nls")
public final class AOpenDataJoinParam extends PJoinParam
{
    private POpenData _openData_;

    public AOpenDataJoinParam()
    {
        // Constructor
    }

    public AOpenDataJoinParam(
        @SuppressWarnings("hiding") POpenData _openData_)
    {
        // Constructor
        setOpenData(_openData_);

    }

    @Override
    public Object clone()
    {
        return new AOpenDataJoinParam(
            cloneNode(this._openData_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAOpenDataJoinParam(this);
    }

    public POpenData getOpenData()
    {
        return this._openData_;
    }

    public void setOpenData(POpenData node)
    {
        if(this._openData_ != null)
        {
            this._openData_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._openData_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._openData_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._openData_ == child)
        {
            this._openData_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._openData_ == oldChild)
        {
            setOpenData((POpenData) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
