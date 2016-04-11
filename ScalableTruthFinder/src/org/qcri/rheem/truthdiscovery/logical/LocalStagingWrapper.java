package org.qcri.rheem.truthdiscovery.logical;

import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.core.logicallayer.LogicalOperatorWrapper;

/**
 * Created by zoi on 25/1/15.
 */
public class LocalStagingWrapper extends LogicalOperatorWrapper<Object> {

    LocalStaging logOp;

    public LocalStagingWrapper(LocalStaging logOp) {
        this.logOp = logOp;
    }

    @Override
    public String apply(RheemContext context, Object... args) {
        this.logOp.staging(context);
        return "";
    }
}
