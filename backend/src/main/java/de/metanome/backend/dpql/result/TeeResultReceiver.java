package de.metanome.backend.dpql.result;

import de.metanome.engine.api.EngineTable;
import de.metanome.engine.api.result_receiver.EngineResultMetadata;
import de.metanome.engine.api.result_receiver.EngineResultReceiver;
import de.metanome.engine.api.result_receiver.EngineResultReceiverException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Simple fan-out receiver that forwards all events to all delegates.
 */
public final class TeeResultReceiver implements EngineResultReceiver {

  private final List<EngineResultReceiver> delegates;

  public TeeResultReceiver(List<EngineResultReceiver> delegates) {
    this.delegates = delegates == null ? new ArrayList<>() : new ArrayList<>(delegates);
  }

  public TeeResultReceiver(EngineResultReceiver... delegates) {
    this.delegates = delegates == null ? new ArrayList<>() : new ArrayList<>(Arrays.asList(delegates));
  }

  @Override
  public void start(EngineResultMetadata executionMetadata) {
    for (EngineResultReceiver d : delegates) {
      d.start(executionMetadata);
    }
  }

  @Override
  public void startTable(EngineTable table) throws EngineResultReceiverException {
    for (EngineResultReceiver d : delegates) {
      d.startTable(table);
    }
  }

  @Override
  public void receiveRow(List<String> row) throws EngineResultReceiverException {
    for (EngineResultReceiver d : delegates) {
      d.receiveRow(row);
    }
  }

  @Override
  public void endTable() throws EngineResultReceiverException {
    for (EngineResultReceiver d : delegates) {
      d.endTable();
    }
  }

  @Override
  public void finish() {
    for (EngineResultReceiver d : delegates) {
      d.finish();
    }
  }
}
