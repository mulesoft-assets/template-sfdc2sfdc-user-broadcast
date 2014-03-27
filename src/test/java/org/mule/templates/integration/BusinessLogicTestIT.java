package org.mule.templates.integration;

import static junit.framework.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mule.MessageExchangePattern;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.context.notification.NotificationException;
import org.mule.processor.chain.SubflowInterceptingChainLifecycleWrapper;
import org.mule.tck.probe.PollingProber;
import org.mule.tck.probe.Prober;
import org.mule.templates.utils.ListenerProbe;
import org.mule.templates.utils.PipelineSynchronizeListener;

import com.mulesoft.module.batch.BatchTestHelper;

/**
 * The objective of this class is to validate the correct behavior of the Mule Template that make calls to external systems.
 * 
 * The test will invoke the batch process and afterwards check that the users had been correctly created and that the ones that should be filtered are not in
 * the destination sand box.
 * 
 */
public class BusinessLogicTestIT extends AbstractTemplateTestCase {

	private static final String TEMPLATE_NAME = "sfdc2sfdc-user-broadcast";

	private BatchTestHelper helper;

	private static final String POLL_FLOW_NAME = "triggerFlow";

	// TODO - Replace this Email with one of your Test User's mail
	private static final String USER_TO_UPDATE_EMAIL = "btest.one@mulesoft.com";

	private Map<String, Object> userToUpdate;

	private SubflowInterceptingChainLifecycleWrapper retrieveUserFromBFlow;

	protected static final int TIMEOUT = 60;

	private final Prober pollProber = new PollingProber(10000, 1000);
	private final PipelineSynchronizeListener pipelineListener = new PipelineSynchronizeListener(POLL_FLOW_NAME);

	@BeforeClass
	public static void init() {
		System.setProperty("page.size", "1000");

		// Set the frequency between polls to 10 seconds
		System.setProperty("poll.frequencyMillis", "10000");

		// Set the poll starting delay to 20 seconds
		System.setProperty("poll.startDelayMillis", "20000");

		// Setting Default Watermark Expression to query SFDC with
		// LastModifiedDate greater than ten seconds before current time
		System.setProperty("watermark.default.expression", "#[groovy: new Date(System.currentTimeMillis() - 10000).format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\", TimeZone.getTimeZone('UTC'))]");

	}

	@Before
	public void setUp() throws Exception {
		stopFlowSchedulers(POLL_FLOW_NAME);
		registerListeners();

		helper = new BatchTestHelper(muleContext);

		retrieveUserFromBFlow = getSubFlow("retrieveUserFromBFlow");
		retrieveUserFromBFlow.initialise();

		createTestDataInSandBox();
	}

	private void registerListeners() throws NotificationException {
		muleContext.registerListener(pipelineListener);
	}

	private void waitForPollToRun() {
		pollProber.check(new ListenerProbe(pipelineListener));
	}

	@SuppressWarnings("unchecked")
	private void createTestDataInSandBox() throws MuleException, Exception {

		SubflowInterceptingChainLifecycleWrapper retrieveUserFromAFlow = getSubFlow("retrieveUserFromAFlow");
		retrieveUserFromAFlow.initialise();

		Map<String, Object> userToRetrieveMail = new HashMap<String, Object>();
		userToRetrieveMail.put("Email", USER_TO_UPDATE_EMAIL);

		MuleEvent event = retrieveUserFromAFlow.process(getTestEvent(userToRetrieveMail, MessageExchangePattern.REQUEST_RESPONSE));

		userToUpdate = (Map<String, Object>) event.getMessage()
													.getPayload();

		userToUpdate.remove("type");
		userToUpdate.put("FirstName", buildUniqueName(TEMPLATE_NAME, "FN"));
		userToUpdate.put("Title", buildUniqueName(TEMPLATE_NAME, "Title"));

		SubflowInterceptingChainLifecycleWrapper updateUserInAFlow = getSubFlow("updateUserInAFlow");
		updateUserInAFlow.initialise();

		List<Map<String, Object>> userList = new ArrayList<Map<String, Object>>();
		userList.add(userToUpdate);

		event = updateUserInAFlow.process(getTestEvent(userList, MessageExchangePattern.REQUEST_RESPONSE));

	}

	protected String buildUniqueName(String templateName, String name) {
		String timeStamp = new Long(new Date().getTime()).toString();

		StringBuilder builder = new StringBuilder();
		builder.append(name);
		builder.append(templateName);
		builder.append(timeStamp);

		return builder.toString();
	}

	@After
	public void tearDown() throws Exception {

		stopFlowSchedulers(POLL_FLOW_NAME);

		// Data from Sandbox should be cleaned in this step but since Users were not created (As they cannot be deleted) nothing is done

	}

	@Test
	public void testMainFlow() throws Exception {

		// Run poll and wait for it to run
		runSchedulersOnce(POLL_FLOW_NAME);
		waitForPollToRun();

		// Wait for the batch job executed by the poll flow to finish
		helper.awaitJobTermination(TIMEOUT_SEC * 1000, 500);
		helper.assertJobWasSuccessful();

		Map<String, Object> userToRetrieveMail = new HashMap<String, Object>();
		userToRetrieveMail.put("Email", USER_TO_UPDATE_EMAIL);

		MuleEvent event = retrieveUserFromBFlow.process(getTestEvent(userToRetrieveMail, MessageExchangePattern.REQUEST_RESPONSE));

		Map<String, Object> payload = (Map<String, Object>) event.getMessage()
																	.getPayload();

		assertEquals("The user should have been sync and new name must match", userToUpdate.get("FirstName"), payload.get("FirstName"));

		assertEquals("The user should have been sync and new title must match", userToUpdate.get("Title"), payload.get("Title"));

	}

}
