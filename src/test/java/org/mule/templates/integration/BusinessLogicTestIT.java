/**
 * Mule Anypoint Template
 * Copyright (c) MuleSoft, Inc.
 * All rights reserved.  http://www.mulesoft.com
 */

package org.mule.templates.integration;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mule.MessageExchangePattern;
import org.mule.api.MuleEvent;
import org.mule.context.notification.NotificationException;
import org.mule.templates.utils.ListenerProbe;

import com.mulesoft.module.batch.BatchTestHelper;

/**
 * The objective of this class is to validate the correct behavior of the Mule Template that make calls to external systems.
 * 
 * The test will invoke the batch process and afterwards check that the users had been correctly created and that the ones that should be filtered are not in
 * the destination sand box.
 * 
 */
public class BusinessLogicTestIT extends AbstractTemplateTestCase {

	// TODO - Replace this Email with one of your Test User's mail
	private static final String USER_TO_UPDATE_EMAIL = "noreply@chatter.salesforce.com";
	private BatchTestHelper helper;
	private Map<String, Object> userToUpdate;

	@BeforeClass
	public static void beforeClass() {
		// Trigger policy is set to poll.
		System.setProperty("trigger.policy", "poll");
		// Setting Default Watermark Expression to query SFDC with
		// LastModifiedDate greater than ten seconds before current time
		System.setProperty("watermark.default.expression", "#[groovy: new Date(System.currentTimeMillis() - 10000).format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\", TimeZone.getTimeZone('UTC'))]");
	}

	@AfterClass
	public static void shutDown() {
		System.clearProperty("trigger.policy");
		System.clearProperty("watermark.default.expression");
	}
	
	@Before
	public void setUp() throws Exception {
		stopFlowSchedulers(POLL_FLOW_NAME);
		registerListeners();

		helper = new BatchTestHelper(muleContext);

		// Flow to retrieve/update users in target system.
		initialiseSubFlows();

		// Prepare test data.
		createTestDataInSandBox();
	}
	
	/**
	 * Inits all tests sub-flows.
	 * @throws Exception when initialisation is unsuccessful 
	 */
	private void initialiseSubFlows() throws Exception {
		retrieveUserFromBFlow = getSubFlow("retrieveUserFromBFlow");
		retrieveUserFromBFlow.initialise();
				
		retrieveUserFromAFlow = getSubFlow("retrieveUserFromAFlow");
		retrieveUserFromAFlow.initialise();
			
		updateUserInAFlow = getSubFlow("updateUserInAFlow");
		updateUserInAFlow.initialise();
	}

	@After
	public void tearDown() throws Exception {
		stopFlowSchedulers(POLL_FLOW_NAME);
		// Data from sandbox should be cleaned in this step but since Users were not created (As they cannot be deleted) nothing is done.
		// Only User data is updated(by email).
	}
	
	private void registerListeners() throws NotificationException {
		muleContext.registerListener(pipelineListener);
	}

	private void waitForPollToRun() {
		pollProber.check(new ListenerProbe(pipelineListener));
	}

	/**
	 * Prepares test data in a sandbox. Consists of obtaining user by email, modification of fetched user's data to be polled 
	 * by a broadcast flow.
	 * @throws Exception when errors in flow occurred.
	 */
	@SuppressWarnings("unchecked")
	private void createTestDataInSandBox() throws Exception {

		Map<String, Object> userToRetrieveMail = new HashMap<String, Object>();
		userToRetrieveMail.put("Email", USER_TO_UPDATE_EMAIL);

		MuleEvent event = retrieveUserFromAFlow.process(getTestEvent(userToRetrieveMail, MessageExchangePattern.REQUEST_RESPONSE));

		userToUpdate = (Map<String, Object>) event.getMessage().getPayload();

		userToUpdate.remove("type");
		userToUpdate.put("FirstName", buildUniqueName(TEMPLATE_NAME, "FN"));
		userToUpdate.put("Title", buildUniqueName(TEMPLATE_NAME, "Title"));

		List<Map<String, Object>> userList = new ArrayList<Map<String, Object>>();
		userList.add(userToUpdate);

		event = updateUserInAFlow.process(getTestEvent(userList, MessageExchangePattern.REQUEST_RESPONSE));
	}

	/**
	 * Builds unique name based on template name and time stamp.
	 */
	protected String buildUniqueName(String templateName, String name) {
		String timeStamp = new Long(new Date().getTime()).toString();
		StringBuilder builder = new StringBuilder();
		builder.append(name);
		builder.append(templateName);
		builder.append(timeStamp);

		return builder.toString();
	}

	/**
	 * Test for a main flow - executes batch processing.
	 * @throws Exception when errors in flow occurred. 
	 */
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

		Map<String, Object> payload = (Map<String, Object>) event.getMessage().getPayload();

		assertEquals("The user should have been sync and new title must match", userToUpdate.get("Title"), payload.get("Title"));
		assertEquals("The user should have been sync and new name must match", userToUpdate.get("FirstName"), payload.get("FirstName"));
	}

}
