/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class APastedDataImportBlock extends PImportBlock
{
    private PPastedData _pastedData_;

    public APastedDataImportBlock()
    {
        // Constructor
    }

    public APastedDataImportBlock(
        @SuppressWarnings("hiding") PPastedData _pastedData_)
    {
        // Constructor
        setPastedData(_pastedData_);

    }

    @Override
    public Object clone()
    {
        return new APastedDataImportBlock(
            cloneNode(this._pastedData_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAPastedDataImportBlock(this);
    }

    public PPastedData getPastedData()
    {
        return this._pastedData_;
    }

    public void setPastedData(PPastedData node)
    {
        if(this._pastedData_ != null)
        {
            this._pastedData_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._pastedData_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._pastedData_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._pastedData_ == child)
        {
            this._pastedData_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._pastedData_ == oldChild)
        {
            setPastedData((PPastedData) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
