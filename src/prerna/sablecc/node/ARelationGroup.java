/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.Analysis;

@SuppressWarnings("nls")
public final class ARelationGroup extends PRelationGroup
{
    private TComma _comma_;
    private PRelationDef _relationDef_;

    public ARelationGroup()
    {
        // Constructor
    }

    public ARelationGroup(
        @SuppressWarnings("hiding") TComma _comma_,
        @SuppressWarnings("hiding") PRelationDef _relationDef_)
    {
        // Constructor
        setComma(_comma_);

        setRelationDef(_relationDef_);

    }

    @Override
    public Object clone()
    {
        return new ARelationGroup(
            cloneNode(this._comma_),
            cloneNode(this._relationDef_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseARelationGroup(this);
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

    public PRelationDef getRelationDef()
    {
        return this._relationDef_;
    }

    public void setRelationDef(PRelationDef node)
    {
        if(this._relationDef_ != null)
        {
            this._relationDef_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._relationDef_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._comma_)
            + toString(this._relationDef_);
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

        if(this._relationDef_ == child)
        {
            this._relationDef_ = null;
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

        if(this._relationDef_ == oldChild)
        {
            setRelationDef((PRelationDef) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
