/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.Analysis;

@SuppressWarnings("nls")
public final class AHtmlWordOrNumOrNestedObj extends PWordOrNumOrNestedObj
{
    private THtmlText _htmlText_;

    public AHtmlWordOrNumOrNestedObj()
    {
        // Constructor
    }

    public AHtmlWordOrNumOrNestedObj(
        @SuppressWarnings("hiding") THtmlText _htmlText_)
    {
        // Constructor
        setHtmlText(_htmlText_);

    }

    @Override
    public Object clone()
    {
        return new AHtmlWordOrNumOrNestedObj(
            cloneNode(this._htmlText_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAHtmlWordOrNumOrNestedObj(this);
    }

    public THtmlText getHtmlText()
    {
        return this._htmlText_;
    }

    public void setHtmlText(THtmlText node)
    {
        if(this._htmlText_ != null)
        {
            this._htmlText_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._htmlText_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._htmlText_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._htmlText_ == child)
        {
            this._htmlText_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._htmlText_ == oldChild)
        {
            setHtmlText((THtmlText) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
