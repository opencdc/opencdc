package deltad.core;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.StringTokenizer;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import deltad.core.api.MailConfiguration;
import deltad.core.mcm.MailConfigureManagerMBean;



/**
 * @author haozhu
 *
 */
public class DefaultServiceEventListener implements ServiceEventListener {

	private static final Log LOGGER = LogFactory.getLog(DefaultServiceEventListener.class.getName());
	
	private final MailConfigureManagerMBean manager;
	
	public DefaultServiceEventListener(MailConfigureManagerMBean man) {
		this.manager = man;
	}
	@Override
	public void handleEvent(ServiceEvent event) {
		if (event instanceof ExceptionEvent) {
			ExceptionEvent exceptionEvent = (ExceptionEvent)event;
			Service service = exceptionEvent.getService();
			String mailConfig = service.getMailConfigurationName();
			if (null == mailConfig || mailConfig.length() == 0)
				return;
			
			if (service.getState().equals(ServiceState.SUSPENDED)) {
				String subject = "ACTION NEEDED, Service " + service + " Suspended";
				StringBuffer body = new StringBuffer(subject).append("\n\n");
				try {
					body.append("------Operation Details--------").append("\n");
					body.append("Suspended Service : ").append(service.getServiceName()).append("\n");
					body.append("Recover Info : ").append(service.getBlockingTXAsString()).append("\n");	
					sendMail(mailConfig, subject, body.toString());
				} catch (Exception e) {
					LOGGER.error("Fail to send a mail.\n" + body.toString(), e);
				}
			}
		}
	}
	
	private void sendMail(String mailConfig, String from, String tolist, String cclist, String subject, String text) throws AddressException, MessagingException, UnsupportedEncodingException {
		Message mailMessage = new MimeMessage(manager.getMailSession(mailConfig));
		mailMessage.setFrom(new InternetAddress(from));
		StringTokenizer st = new StringTokenizer(tolist, ",");
		while (st.hasMoreElements()) {
			String to = st.nextToken();
			mailMessage.addRecipient(Message.RecipientType.TO, new InternetAddress(to));
		}
		
		st = new StringTokenizer(cclist, ",");
		while (st.hasMoreElements()) {
			String cc = st.nextToken();
			mailMessage.addRecipient(Message.RecipientType.CC, new InternetAddress(cc));
		}
		
		mailMessage.setSubject(subject);
		mailMessage.setSentDate(new Date());
		mailMessage.setText(text);
		Transport.send(mailMessage);
	}
	
	private void sendMail(String mailConfigName, String subject, String body) throws AddressException, MessagingException, UnsupportedEncodingException {
		MailConfiguration mailConf = manager.getMailConfigure(mailConfigName);		
		String from = mailConf.getProperty(MailConfiguration.MAIL_FROM_KEY);
		String tolist = mailConf.getProperty(MailConfiguration.MAIL_TO_LIST_KEY);
		String cclist = mailConf.getProperty(MailConfiguration.MAIL_CC_LIST_KEY);
		sendMail(mailConfigName, from, tolist, cclist, subject, body);
	}

}
