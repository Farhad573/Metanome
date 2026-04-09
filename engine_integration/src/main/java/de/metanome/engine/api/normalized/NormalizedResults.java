package de.metanome.engine.api.normalized;

import java.util.List;

/**
 * Optional normalized (structured) results for engines that want to return
 * FD/IND/UCC results in a typed form, in addition to or instead of tabular output.
 */
public class NormalizedResults {

    private List<NormalizedFD> fds;
    private List<NormalizedIND> inds;
    private List<NormalizedUCC> uccs;

    public List<NormalizedFD> getFds() {
        return fds;
    }

    public void setFds(List<NormalizedFD> fds) {
        this.fds = fds;
    }

    public List<NormalizedIND> getInds() {
        return inds;
    }

    public void setInds(List<NormalizedIND> inds) {
        this.inds = inds;
    }

    public List<NormalizedUCC> getUccs() {
        return uccs;
    }

    public void setUccs(List<NormalizedUCC> uccs) {
        this.uccs = uccs;
    }
}
