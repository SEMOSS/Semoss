/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.Analysis;

@SuppressWarnings("nls")
public final class APanelCommentRemovePanelop extends PPanelop
{
    private PPanelCommentRemove _panelCommentRemove_;

    public APanelCommentRemovePanelop()
    {
        // Constructor
    }

    public APanelCommentRemovePanelop(
        @SuppressWarnings("hiding") PPanelCommentRemove _panelCommentRemove_)
    {
        // Constructor
        setPanelCommentRemove(_panelCommentRemove_);

    }

    @Override
    public Object clone()
    {
        return new APanelCommentRemovePanelop(
            cloneNode(this._panelCommentRemove_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAPanelCommentRemovePanelop(this);
    }

    public PPanelCommentRemove getPanelCommentRemove()
    {
        return this._panelCommentRemove_;
    }

    public void setPanelCommentRemove(PPanelCommentRemove node)
    {
        if(this._panelCommentRemove_ != null)
        {
            this._panelCommentRemove_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._panelCommentRemove_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._panelCommentRemove_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._panelCommentRemove_ == child)
        {
            this._panelCommentRemove_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._panelCommentRemove_ == oldChild)
        {
            setPanelCommentRemove((PPanelCommentRemove) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
