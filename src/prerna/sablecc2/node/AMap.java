/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import prerna.sablecc2.analysis.Analysis;

@SuppressWarnings("nls")
public final class AMap extends PMap
{
    private TLCurl _lCurl_;
    private PMapEntry _mapEntry_;
    private final LinkedList<POtherMapEntry> _otherMapEntry_ = new LinkedList<POtherMapEntry>();
    private TRCurl _rCurl_;

    public AMap()
    {
        // Constructor
    }

    public AMap(
        @SuppressWarnings("hiding") TLCurl _lCurl_,
        @SuppressWarnings("hiding") PMapEntry _mapEntry_,
        @SuppressWarnings("hiding") List<?> _otherMapEntry_,
        @SuppressWarnings("hiding") TRCurl _rCurl_)
    {
        // Constructor
        setLCurl(_lCurl_);

        setMapEntry(_mapEntry_);

        setOtherMapEntry(_otherMapEntry_);

        setRCurl(_rCurl_);

    }

    @Override
    public Object clone()
    {
        return new AMap(
            cloneNode(this._lCurl_),
            cloneNode(this._mapEntry_),
            cloneList(this._otherMapEntry_),
            cloneNode(this._rCurl_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAMap(this);
    }

    public TLCurl getLCurl()
    {
        return this._lCurl_;
    }

    public void setLCurl(TLCurl node)
    {
        if(this._lCurl_ != null)
        {
            this._lCurl_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._lCurl_ = node;
    }

    public PMapEntry getMapEntry()
    {
        return this._mapEntry_;
    }

    public void setMapEntry(PMapEntry node)
    {
        if(this._mapEntry_ != null)
        {
            this._mapEntry_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._mapEntry_ = node;
    }

    public LinkedList<POtherMapEntry> getOtherMapEntry()
    {
        return this._otherMapEntry_;
    }

    public void setOtherMapEntry(List<?> list)
    {
        for(POtherMapEntry e : this._otherMapEntry_)
        {
            e.parent(null);
        }
        this._otherMapEntry_.clear();

        for(Object obj_e : list)
        {
            POtherMapEntry e = (POtherMapEntry) obj_e;
            if(e.parent() != null)
            {
                e.parent().removeChild(e);
            }

            e.parent(this);
            this._otherMapEntry_.add(e);
        }
    }

    public TRCurl getRCurl()
    {
        return this._rCurl_;
    }

    public void setRCurl(TRCurl node)
    {
        if(this._rCurl_ != null)
        {
            this._rCurl_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._rCurl_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._lCurl_)
            + toString(this._mapEntry_)
            + toString(this._otherMapEntry_)
            + toString(this._rCurl_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._lCurl_ == child)
        {
            this._lCurl_ = null;
            return;
        }

        if(this._mapEntry_ == child)
        {
            this._mapEntry_ = null;
            return;
        }

        if(this._otherMapEntry_.remove(child))
        {
            return;
        }

        if(this._rCurl_ == child)
        {
            this._rCurl_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._lCurl_ == oldChild)
        {
            setLCurl((TLCurl) newChild);
            return;
        }

        if(this._mapEntry_ == oldChild)
        {
            setMapEntry((PMapEntry) newChild);
            return;
        }

        for(ListIterator<POtherMapEntry> i = this._otherMapEntry_.listIterator(); i.hasNext();)
        {
            if(i.next() == oldChild)
            {
                if(newChild != null)
                {
                    i.set((POtherMapEntry) newChild);
                    newChild.parent(this);
                    oldChild.parent(null);
                    return;
                }

                i.remove();
                oldChild.parent(null);
                return;
            }
        }

        if(this._rCurl_ == oldChild)
        {
            setRCurl((TRCurl) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
