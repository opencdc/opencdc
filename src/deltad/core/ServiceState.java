package deltad.core;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public enum ServiceState implements Serializable {
	RUNNING("Running"), SUSPENDED("Suspended"), STOPPED("STOPPED");

	private final String state;
	private static final Map<String, ServiceState> nameMap;

	static {
		nameMap = new HashMap<String, ServiceState>();
		for (ServiceState type : values()) {
			nameMap.put(type.getStateName(), type);
		}
	}

	private ServiceState(String state) {
		this.state = state;
	}

	public String getStateName() {
		return state;
	}

	public static ServiceState fromProductName(String state) {
		if (!nameMap.containsKey(state)) {
			throw new IllegalArgumentException("ServiceState not found for state name: [" + state + "]");
		} else {
			return nameMap.get(state);
		}
	}

}
