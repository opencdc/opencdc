package deltad.core;

import deltad.core.api.DeltaTransaction;

public class LinkedDeltaTransaction extends DeltaTransaction {
	private static final long serialVersionUID = -3434472133431709402L;
	
	LinkedDeltaTransaction previous;
	LinkedDeltaTransaction next;

	public LinkedDeltaTransaction(String xid) {
		super(xid);
	}
	
	public LinkedDeltaTransaction(String xid, int branchID, int branchn) {
		super(xid, branchID, branchn);
	}

	public LinkedDeltaTransaction getPrevious() {
		return previous;
	}

	public void setPrevious(LinkedDeltaTransaction previous) {
		this.previous = previous;
	}

	public LinkedDeltaTransaction getNext() {
		return next;
	}

	public void setNext(LinkedDeltaTransaction next) {
		this.next = next;
	}

}
