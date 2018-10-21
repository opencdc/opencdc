package deltad.core;


public class ExceptionEvent implements ServiceEvent {
	
	private final Service service;
	private final Exception exception;
	private final Object attachment;
	
	public ExceptionEvent(Service service, Exception e, Object attachment) {
		this.service = service;
		this.exception = e;
		this.attachment = attachment;
	}

	@Override
	public Service getService() {
		return service;
	}
	
	public Exception getException() {
		return exception;
	}
	
	public Object getAttachment() {
		return attachment;
	}

}
