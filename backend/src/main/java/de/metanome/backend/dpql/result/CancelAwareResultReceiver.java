package de.metanome.backend.dpql.result;

import de.metanome.engine.api.CancellationToken;
import de.metanome.engine.api.EngineTable;
import de.metanome.engine.api.result_receiver.EngineResultMetadata;
import de.metanome.engine.api.result_receiver.EngineResultReceiver;
import de.metanome.engine.api.result_receiver.EngineResultReceiverException;

import java.util.List;

/**
 * Receiver wrapper that enforces cancellation by throwing once the token is canceled.
 *
 * This makes cancellation effective even for engines that do not explicitly check interrupts,
 * as long as they emit at least one row/batch after cancellation is requested.
 */
public final class CancelAwareResultReceiver implements EngineResultReceiver {

    private final EngineResultReceiver delegate;
    private final CancellationToken token;

    public CancelAwareResultReceiver(EngineResultReceiver delegate, CancellationToken token) {
        this.delegate = delegate;
        this.token = token;
    }

    private void checkCanceled() throws EngineResultReceiverException {
        if (token != null && token.isCanceled()) {
            throw new EngineResultReceiverException("Execution canceled");
        }
    }

    @Override
    public void start(EngineResultMetadata executionMetadata) {
        if (delegate != null) {
            delegate.start(executionMetadata);
        }
    }

    @Override
    public void startTable(EngineTable table) throws EngineResultReceiverException {
        checkCanceled();
        if (delegate != null) {
            delegate.startTable(table);
        }
    }

    @Override
    public void receiveRow(List<String> row) throws EngineResultReceiverException {
        checkCanceled();
        if (delegate != null) {
            delegate.receiveRow(row);
        }
    }

    @Override
    public void endTable() throws EngineResultReceiverException {
        checkCanceled();
        if (delegate != null) {
            delegate.endTable();
        }
    }

    @Override
    public void finish() {
        if (delegate != null) {
            delegate.finish();
        }
    }
}

