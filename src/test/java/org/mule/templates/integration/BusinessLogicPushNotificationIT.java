/**
 * Mule Anypoint Template
 * Copyright (c) MuleSoft, Inc.
 * All rights reserved.  http://www.mulesoft.com
 */

package org.mule.templates.integration;

import static org.junit.Assert.*;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mule.MessageExchangePattern;
import org.mule.api.MuleEvent;
import org.mule.construct.Flow;
import org.mule.context.notification.NotificationException;
import org.mule.transformer.types.DataTypeFactory;

import com.mulesoft.module.batch.BatchTestHelper;

/**
 * The objective of this class is to validate the correct behavior of the
 * Anypoint Template that make calls to external systems.
 * 
 * @author Vlado Andoga
 */
@SuppressWarnings("unchecked")
public class BusinessLogicPushNotificationIT extends AbstractTemplateTestCase {
	
	private static final int TIMEOUT_MILLIS = 60;
	private static final String USER_EMAIL = "First@email.sk";
	private BatchTestHelper helper;
	private Flow triggerPushFlow;
	
	@BeforeClass
	public static void beforeClass() {
		System.setProperty("trigger.policy", "push");		
	}

	@AfterClass
	public static void shutDown() {
		System.clearProperty("trigger.policy");
	}

	@Before
	public void setUp() throws Exception {
		stopFlowSchedulers(POLL_FLOW_NAME);
		registerListeners();
		helper = new BatchTestHelper(muleContext);
		triggerPushFlow = getFlow("triggerPushFlow");
		initialiseSubFlows();
	}

	@After
	public void tearDown() throws Exception {
		stopFlowSchedulers(POLL_FLOW_NAME);
	}
	
	/**
	 * Inits all tests sub-flows.
	 * @throws Exception when initialisation is unsuccessful 
	 */
	private void initialiseSubFlows() throws Exception {
		retrieveUserFromBFlow = getSubFlow("retrieveUserFromBFlow");
		retrieveUserFromBFlow.initialise();
		
		retrieveUserByNameFromBFlow = getSubFlow("retrieveUserByNameFromBFlow");
		retrieveUserByNameFromBFlow.initialise();
	}
	
	private void registerListeners() throws NotificationException {
		muleContext.registerListener(pipelineListener);
	}


	/**
	 * In test, we are creating new SOAP message to create/update an existing user. User first name is always generated
	 * to ensure, that flow correctly updates user in the Saleforce. 
	 * @throws Exception whe flow error occurred
	 */
	@Test
	public void testMainFlow() throws Exception {
		// Execution
		String firstName = buildUniqueName();
		
		final MuleEvent testEvent = getTestEvent(null, triggerPushFlow);
		testEvent.getMessage().setPayload(buildRequest(firstName), DataTypeFactory.create(InputStream.class, "application/xml"));
		
		triggerPushFlow.process(testEvent);
		
		helper.awaitJobTermination(TIMEOUT_MILLIS * 1000, 500);
		helper.assertJobWasSuccessful();

		Map<String, Object> userToRetrieveByName = new HashMap<String, Object>();
		userToRetrieveByName.put("FirstName", firstName);

		final MuleEvent retrieveEvent = retrieveUserByNameFromBFlow.process(getTestEvent(userToRetrieveByName, MessageExchangePattern.REQUEST_RESPONSE));

		Map<String, Object> payload = (Map<String, Object>) retrieveEvent.getMessage().getPayload();

		// Assertion, if the upserted user was found
		assertTrue(payload.size()> 0);
	}

	/**
	 * Builds the soap request as a string
	 * @param firstName the first name
	 * @return a soap message as string
	 */
	private String buildRequest(String firstName){
		StringBuilder request = new StringBuilder();
		request.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
		request.append("<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">");
		request.append("<soapenv:Body>");
		request.append("  <notifications xmlns=\"http://soap.sforce.com/2005/09/outbound\">");
		request.append("   <OrganizationId>00Dd0000000dtDqEAI</OrganizationId>");
		request.append("   <ActionId>04kd0000000PCgvAAG</ActionId>");
		request.append("   <SessionId xsi:nil=\"true\"/>");
		request.append("   <EnterpriseUrl>https://na14.salesforce.com/services/Soap/c/30.0/00Dd0000000dtDq</EnterpriseUrl>");
		request.append("   <PartnerUrl>https://na14.salesforce.com/services/Soap/u/30.0/00Dd0000000dtDq</PartnerUrl>");
		request.append("   <Notification>");
		//request.append("    <Id>001d000001XD5XKAA2</Id>");
		request.append("    <sObject xsi:type=\"sf:User\" xmlns:sf=\"sf:sobject.enterprise.soap.sforce.com\">");
		//request.append("      <sf:Id>001d000001XD5XKAA2</sf:Id>");
		request.append("      <sf:ProfileId>00e80000001CDZBAA4</sf:ProfileId>");
		request.append("      <sf:AboutMe>About me</sf:AboutMe>");
		request.append("      <sf:Alias>First</sf:Alias>");
		request.append("      <sf:City>Kosice</sf:City>");
		request.append("      <sf:CommunityNickname>Community name</sf:CommunityNickname>");
		request.append("      <sf:CompanyName>Hotovo</sf:CompanyName>");
		request.append("      <sf:Country>Slovakia</sf:Country>");
		request.append("      <sf:Email>" +USER_EMAIL+ "</sf:Email>");
		request.append("      <sf:EmailEncodingKey>UTF-8</sf:EmailEncodingKey>");
		request.append("      <sf:EmployeeNumber>100</sf:EmployeeNumber>");
		request.append("      <sf:FirstName>" + firstName + "</sf:FirstName>");
		request.append("      <sf:LastName>Last</sf:LastName>");
		request.append("      <sf:Username>Name" +USER_EMAIL+ "</sf:Username>");
		request.append("      <sf:Title>Msc</sf:Title>");
		request.append("      <sf:TimeZoneSidKey>Europe/Dublin</sf:TimeZoneSidKey>");
		request.append("      <sf:LocaleSidKey>en_US</sf:LocaleSidKey>");
		request.append("      <sf:LanguageLocaleKey>en_US</sf:LanguageLocaleKey>");
		request.append("    </sObject>");
		request.append("   </Notification>");
		request.append("  </notifications>");
		request.append(" </soapenv:Body>");
		request.append("</soapenv:Envelope>");
		return request.toString();
	}
	
	/**
	 * Builds unique name based on current time stamp.
	 * @return a unique name as string
	 */
	private String buildUniqueName() {
		return TEMPLATE_NAME + "-" + System.currentTimeMillis();
	}
	
}
