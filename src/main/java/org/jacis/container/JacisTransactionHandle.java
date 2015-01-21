package org.jacis.container;

/**
 * @author Jan Wiemer
 *
 * Jacis handle of a transaction.
 */
public class JacisTransactionHandle {

  private final String txName;
  private final Object externalTransaction;

  public JacisTransactionHandle(String txName, Object externalTransaction) {
    this.txName = txName;
    this.externalTransaction = externalTransaction;
  }

  public String getTxName() {
    return txName;
  }

  public Object getExternalTransaction() {
    return externalTransaction;
  }

  @Override
  public int hashCode() {
    return externalTransaction.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null) {
      return false;
    } else if (getClass() != obj.getClass()) {
      return false;
    }
    JacisTransactionHandle that = (JacisTransactionHandle) obj;
    return externalTransaction == null ? that.externalTransaction == null : externalTransaction.equals(that.externalTransaction);
  }

  @Override
  public String toString() {
    return "TX(" + txName + ")";
  }

}