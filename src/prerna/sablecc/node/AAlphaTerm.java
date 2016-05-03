/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class AAlphaTerm extends PTerm
{
    private TWord _word_;

    public AAlphaTerm()
    {
        // Constructor
    }

    public AAlphaTerm(
        @SuppressWarnings("hiding") TWord _word_)
    {
        // Constructor
        setWord(_word_);

    }

    @Override
    public Object clone()
    {
        return new AAlphaTerm(
            cloneNode(this._word_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAAlphaTerm(this);
    }

    public TWord getWord()
    {
        return this._word_;
    }

    public void setWord(TWord node)
    {
        if(this._word_ != null)
        {
            this._word_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._word_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._word_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._word_ == child)
        {
            this._word_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._word_ == oldChild)
        {
            setWord((TWord) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
