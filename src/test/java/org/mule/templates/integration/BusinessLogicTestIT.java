package org.mule.templates.integration;

import org.mule.templates.builders.SfdcObjectBuilder;
import org.mule.templates.integration.AbstractTemplateTestCase;
import org.mule.templates.test.utils.PipelineSynchronizeListener;
import org.mule.templates.test.utils.ListenerProbe;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.mule.templates.builders.SfdcObjectBuilder.aUser;

import java.text.MessageFormat;
import java.util.ArrayList;
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
import org.mule.api.context.notification.ServerNotification;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.construct.Flow;
import org.mule.context.notification.NotificationException;
import org.mule.modules.salesforce.bulk.EnrichedUpsertResult;
import org.mule.processor.chain.SubflowInterceptingChainLifecycleWrapper;
import org.mule.tck.probe.PollingProber;
import org.mule.tck.probe.Probe;
import org.mule.tck.probe.Prober;
import org.mule.transport.NullPayload;

import com.mulesoft.module.batch.BatchTestHelper;
import com.mulesoft.module.batch.api.BatchJobInstance;
import com.mulesoft.module.batch.api.notification.BatchNotification;
import com.mulesoft.module.batch.api.notification.BatchNotificationListener;
import com.mulesoft.module.batch.engine.BatchJobInstanceAdapter;
import com.mulesoft.module.batch.engine.BatchJobInstanceStore;

/**
 * The objective of this class is to validate the correct behavior of the Mule
 * Template that make calls to external systems.
 * 
 * The test will invoke the batch process and afterwards check that the users
 * had been correctly created and that the ones that should be filtered are not
 * in the destination sand box.
 * 
 */
public class BusinessLogicTestIT extends AbstractTemplateTestCase {

	private static final String TEMPLATE_NAME = "sfdc2sfdc-user-broadcast";

	private BatchTestHelper helper;
	
	// TODO - Replace this ProfileId with one of your own org
	private static final String DEFAULT_PROFILE_ID = "00e80000001CDZBAA4";
	private static final String POLL_FLOW_NAME = "triggerFlow";

	protected static final int TIMEOUT = 60;

	private final Prober pollProber = new PollingProber(10000, 1000);
	private final PipelineSynchronizeListener pipelineListener = new PipelineSynchronizeListener(POLL_FLOW_NAME);

	private static SubflowInterceptingChainLifecycleWrapper checkUserflow;
	private static List<Map<String, Object>> createdUsers = new ArrayList<Map<String, Object>>();

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

		checkOpportunityflow = getSubFlow("retrieveOpportunityFlow");
		checkOpportunityflow.initialise();

		checkAccountflow = getSubFlow("retrieveAccountFlow");
		checkAccountflow.initialise();

		createTestDataInSandBox();
	}
	
	private void registerListeners() throws NotificationException {
		muleContext.registerListener(pipelineListener);
	}

	private void waitForPollToRun() {
		pollProber.check(new ListenerProbe(pipelineListener));
	}


	@After
	public void tearDown() throws Exception {

		stopFlowSchedulers(POLL_FLOW_NAME);
		deleteTestDataFromSandBox();

	}

	@Test
	public void testMainFlow() throws Exception {
		// Run poll and wait for it to run
		runSchedulersOnce(POLL_FLOW_NAME);
		waitForPollToRun();

		// Wait for the batch job executed by the poll flow to finish
		helper.awaitJobTermination(TIMEOUT_SEC * 1000, 500);
		helper.assertJobWasSuccessful();

		assertNull("The user should not have been sync", invokeRetrieveUserFlow(checkUserflow, createdUsers.get(0)));

		Map<String, String> payload = invokeRetrieveUserFlow(checkUserflow, createdUsers.get(1));
		assertEquals("The user should have been sync", createdUsers.get(1).get("Email"), payload.get("Email"));
	}

	@SuppressWarnings("unchecked")
	private Map<String, String> invokeRetrieveUserFlow(SubflowInterceptingChainLifecycleWrapper flow, Map<String, Object> user) throws Exception {
		Map<String, Object> userMap = new HashMap<String, Object>();

		userMap.put("Email", user.get("Email"));
		userMap.put("FirstName", user.get("FirstName"));
		userMap.put("LastName", user.get("LastName"));

		MuleEvent event = flow.process(getTestEvent(userMap, MessageExchangePattern.REQUEST_RESPONSE));
		Object payload = event.getMessage().getPayload();
		if (payload instanceof NullPayload) {
			return null;
		} else {
			return (Map<String, String>) payload;
		}
	}


	@SuppressWarnings("unchecked")
	private void createTestDataInSandBox() throws MuleException, Exception {
		
		SubflowInterceptingChainLifecycleWrapper flow = getSubFlowAndInitialiseIt("createUserFlow");

		
	SfdcObjectBuilder baseUser = aUser() //
				.with("TimeZoneSidKey", "GMT") //
				.with("LocaleSidKey", "en_US") //
				.with("EmailEncodingKey", "ISO-8859-1") //
				.with("LanguageLocaleKey", "en_US") //
				.with("ProfileId", DEFAULT_PROFILE_ID);

		// This user should not be sync
		createdUsers.add(baseUser //
				.with("FirstName", "FirstName_0") //
				.with("LastName", "LastName_0") //
				.with("Alias", "Alias_0") //
				.with("IsActive", false) //
				.with("Username", generateUnique("some.email.0@fakemail.com")) //
				.with("Email", "some.email.0@fakemail.com") //
				.build());

		// This user should BE sync
		createdUsers.add(baseUser //
				.with("FirstName", "FirstName_1") //
				.with("LastName", "LastName_1") //
				.with("Alias", "Alias_" + 1) //
				.with("IsActive", true) //
				.with("Username", generateUnique("some.email.1@fakemail.com")) //
				.with("Email", "some.email.1@fakemail.com") //
				.build());

		MuleEvent event = flow.process(getTestEvent(createdUsers, MessageExchangePattern.REQUEST_RESPONSE));
		List<EnrichedUpsertResult> results = (List<org.mule.modules.salesforce.bulk.EnrichedUpsertResult>) event.getMessage().getPayload();
		for (int i = 0; i < results.size(); i++) {
			createdUsers.get(i).put("Id", results.get(i).getId());
		}
	}

	private void deleteTestDataFromSandBox() throws MuleException, Exception {
		// Delete the created users in A
		SubflowInterceptingChainLifecycleWrapper flow = getSubFlowAndInitialiseIt("deleteUserFromAFlow");

		List<String> idList = new ArrayList<String>();
		for (Map<String, Object> c : createdUsers) {
			idList.add((String) c.get("Id"));
		}
		flow.process(getTestEvent(idList, MessageExchangePattern.REQUEST_RESPONSE));

		// Delete the created users in B
		flow = getSubFlowAndInitialiseIt("deleteUserFromBFlow");

		idList.clear();
		for (Map<String, Object> c : createdUsers) {
			Map<String, String> user = invokeRetrieveUserFlow(checkUserflow, c);
			if (user != null) {
				idList.add(user.get("Id"));
			}
		}
		flow.process(getTestEvent(idList, MessageExchangePattern.REQUEST_RESPONSE));
	}

	private SubflowInterceptingChainLifecycleWrapper getSubFlowAndInitialiseIt(String name) throws InitialisationException {
		SubflowInterceptingChainLifecycleWrapper subFlow = getSubFlow(name);
		subFlow.initialise();
		return subFlow;
	}

	private String generateUnique(String string) {
		return MessageFormat.format("{0}-{1}-{2}", TEMPLATE_NAME, System.currentTimeMillis(), string);
	}

}
