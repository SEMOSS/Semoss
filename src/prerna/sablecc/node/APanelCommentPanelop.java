/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class APanelCommentPanelop extends PPanelop
{
    private PPanelComment _panelComment_;

    public APanelCommentPanelop()
    {
        // Constructor
    }

    public APanelCommentPanelop(
        @SuppressWarnings("hiding") PPanelComment _panelComment_)
    {
        // Constructor
        setPanelComment(_panelComment_);

    }

    @Override
    public Object clone()
    {
        return new APanelCommentPanelop(
            cloneNode(this._panelComment_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAPanelCommentPanelop(this);
    }

    public PPanelComment getPanelComment()
    {
        return this._panelComment_;
    }

    public void setPanelComment(PPanelComment node)
    {
        if(this._panelComment_ != null)
        {
            this._panelComment_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._panelComment_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._panelComment_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._panelComment_ == child)
        {
            this._panelComment_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._panelComment_ == oldChild)
        {
            setPanelComment((PPanelComment) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
