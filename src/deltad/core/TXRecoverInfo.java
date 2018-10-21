package deltad.core;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class TXRecoverInfo implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private String blockingXID = null;
	private String info;
	private Exception cause;
	private Set<Integer> branchIDs = new HashSet<Integer>();
	
	public TXRecoverInfo(String xid) {
		blockingXID = xid;
	}
	
	public String getBlockingXID() {
		return blockingXID;
	}
	public void setBlockingXID(String blockingXID) {
		this.blockingXID = blockingXID;
	}	

	public String getInfo() {
		return info;
	}

	public void setInfo(String info) {
		this.info = info;
	}
	
	public Exception getCause() {
		return cause;
	}

	public void setCause(Exception cause) {
		this.cause = cause;
	}
	
	public void removeBranch(int index) {
		this.branchIDs.remove(new Integer(index));
	}
	
	public void addBranch(int branchId) {
		this.branchIDs.add(new Integer(branchId));
	}
	
	public boolean containsBranch(int index) {
		return branchIDs.contains(new Integer(index));
	}
	
	public int branchSize() {
		return branchIDs.size();
	}
	
	public void clean() {
		this.blockingXID = null;
		this.branchIDs.clear();
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(info);
		sb.append("\nCause :").append(cause.getClass().getName()).append(" : ").append(cause.getMessage());
		sb.append("\nXID: ").append(blockingXID).append(" :");
		for (Integer i : branchIDs) {
			sb.append(i.intValue()).append(" ");
		}
		
		return sb.toString(); 
	}
	
}