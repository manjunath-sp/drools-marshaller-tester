package drools.marshallng.tester;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.drools.core.ClockType;
import org.drools.core.TimerJobFactoryType;
import org.drools.core.spi.ConsequenceException;
import org.drools.core.time.SessionPseudoClock;
import org.junit.Assert;
import org.junit.Test;
import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.Message;
import org.kie.api.builder.Results;
import org.kie.api.builder.model.KieBaseModel;
import org.kie.api.builder.model.KieModuleModel;
import org.kie.api.builder.model.KieSessionModel;
import org.kie.api.conf.EventProcessingOption;
import org.kie.api.marshalling.Marshaller;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.KieSessionConfiguration;
import org.kie.api.runtime.conf.ClockTypeOption;
import org.kie.api.runtime.conf.TimedRuleExecutionOption;
import org.kie.api.runtime.conf.TimerJobFactoryOption;
import org.kie.api.runtime.rule.EntryPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import drools.marshaller.tester.Account;

public class ReproducerTest {

	private KieContainer kieContainer;

	private static final String K_BASE_NAME = "KBASE";
	private static final String SESSION_NAME = "KSession_MODEL_A";
	private static final String RULES_PACKAGE = "org.drools.examples.banking";

	private static final String RULE_FILE =  "Example.drl";
	private static final String OUTPUT_DRL = "src" + File.separator + "main" + File.separator + "resources"
			+ File.separator + "org" + File.separator + "drools" + File.separator + "examples" + File.separator
			+ "banking" + File.separator + "Example.drl";
	private static final Logger LOGGER = LoggerFactory.getLogger(ReproducerTest.class);

	@Test
	public void testWithSerilization() throws InterruptedException, ExecutionException, TimeoutException {
		KieSession kSession = createNewkieSession();
		EntryPoint stream = kSession.getEntryPoint("EP");
		SessionPseudoClock clock = kSession.getSessionClock();
		ExecutorService threadPool = Executors.newCachedThreadPool();
		try {

			for (int i = 0; i < 1000; i++) {
				Account account = new Account(i, clock.getCurrentTime());
				stream.insert(account);
				
				Callable<Integer> callable = new Callable<Integer>() {
			        @Override
			        public Integer call() throws Exception {
			          try {
			            return kSession.fireAllRules();
			          } catch (ConsequenceException e) {
			            // Catch and log drools runtime exceptions
			            LOGGER.error("Exception caught while executing rules", e);
			          }
			          return null;
			        }
			      };

			      Future<Integer> future = threadPool.submit(callable);
			      try {
			        // Only back-up after fireAllRules has completed execution, else may throw runtime
			        // exceptions
			        if (future.get().intValue() >= 0) {
			        	backupKieSession(kSession);
			        }
			      } catch (InterruptedException | ExecutionException e) {
			        // Catch and log drools runtime exceptions
			        LOGGER.error("Exception caught while executing rules and backing up rules", e);
			      }
			}

		} finally {
			kSession.halt();
			kSession.dispose();
		}
	}
	
	
	@Test
	public void testWithNoSerilization() throws InterruptedException, ExecutionException, TimeoutException {
		KieSession kSession = createNewkieSession();
		EntryPoint stream = kSession.getEntryPoint("EP");
		SessionPseudoClock clock = kSession.getSessionClock();
		ExecutorService threadPool = Executors.newCachedThreadPool();
		try {

			for (int i = 0; i < 1000; i++) {
				clock.advanceTime(1, TimeUnit.SECONDS);
				Account account = new Account(i, clock.getCurrentTime());
				stream.insert(account);
				
				Callable<Integer> callable = new Callable<Integer>() {
			        @Override
			        public Integer call() throws Exception {
			          try {
			            return kSession.fireAllRules();
			          } catch (ConsequenceException e) {
			            // Catch and log drools runtime exceptions
			            LOGGER.error("Exception caught while executing rules", e);
			          }
			          return null;
			        }
			      };

			      Future<Integer> future = threadPool.submit(callable);
			      try {
			        if (future.get().intValue() >= 0) {
			        	System.out.println("No of rules executed:" + future.get().intValue());
			        }
			      } catch (InterruptedException | ExecutionException e) {
			        // Catch and log drools runtime exceptions
			        LOGGER.error("Exception caught while executing rules and backing up rules", e);
			      }
			}

		} finally {
			kSession.halt();
			kSession.dispose();
		}
		
	}

	/**
	 * Extracts the marshaller - creates a new one.
	 *
	 * 
	 * @return marshaller for the modelName
	 */
	private Marshaller getMarshaller() {

		Marshaller marshaller = null;
		if (kieContainer != null) {

			try {
				KieBase kBase = kieContainer.getKieBase(K_BASE_NAME);

				if (kBase != null) {
					marshaller = KieServices.Factory.get().getMarshallers().newMarshaller(kBase);
				}
			} catch (RuntimeException e) {
				LOGGER.warn("There was an error trying to access the KieBase for modelName:", e);
			}
		}
		return marshaller;
	}

	/**
	 * Backs up the kie session
	 *
	 */
	private void backupKieSession(KieSession kSession) {

		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();) {
			getMarshaller().marshall(baos, kSession);
			//Backup in redis
			System.out.println("The serilaized session:" + baos.toByteArray().toString());

		} catch (IOException e) {
			LOGGER.error("An error occurred while trying to marshal the kie session", e);
		}
	}

	// Returns new KieSession
	private KieSession createNewkieSession() {

		KieServices kieServices = KieServices.Factory.get();
		KieModuleModel kieModuleModel = kieServices.newKieModuleModel();

		KieBaseModel kieBaseModel = kieModuleModel.newKieBaseModel(K_BASE_NAME)
				.setEventProcessingMode(EventProcessingOption.STREAM).setDefault(true).addPackage(RULES_PACKAGE);

		// Need this to create session.
		KieSessionModel kieSessionModel = kieBaseModel.newKieSessionModel(SESSION_NAME);
		kieSessionModel.setClockType(ClockTypeOption.get(ClockType.PSEUDO_CLOCK.getId()));
		KieFileSystem kfs = kieServices.newKieFileSystem();
		kfs.writeKModuleXML(kieModuleModel.toXML());

		URL ruleFile1Url = this.getClass().getClassLoader().getResource(RULE_FILE);
		try (FileInputStream drl = new FileInputStream(ruleFile1Url.getFile())) {

			kfs.write(OUTPUT_DRL, kieServices.getResources().newInputStreamResource(drl));

		} catch (IOException e) {
			Assert.fail("Unable to process rule file:" + RULE_FILE);
		}

		KieBuilder kieBuilder = kieServices.newKieBuilder(kfs).buildAll();

		Results results = kieBuilder.getResults();
		if (results.hasMessages(Message.Level.ERROR)) {
			Assert.fail("Rule has errors:" + results.getMessages().toString());
		}

		kieContainer = kieServices.newKieContainer(kieServices.getRepository().getDefaultReleaseId());

		results = kieContainer.verify();

		if (results.hasMessages(Message.Level.WARNING, Message.Level.ERROR)) {
			Assert.fail("Rule file has errors:" + results.getMessages().toString());
		}
		
		// Configuration that respects time windows when running in passive mode
	    KieSessionConfiguration ksconf = newKieSessionConfiguration();
	      
		KieSession kieSession = kieContainer.newKieSession(SESSION_NAME, ksconf);

		LOGGER.info("Created new kieSession");
		return kieSession;
	}
	
	/**
	   * KieSessionConfiguration used to create a new KieSession and to de-serialize the drools session
	   * 
	   * @return
	   */
	  private KieSessionConfiguration newKieSessionConfiguration() {
	    KieSessionConfiguration ksconf = KieServices.Factory.get().newKieSessionConfiguration();
	    ksconf.setOption(TimerJobFactoryOption.get(TimerJobFactoryType.TRACKABLE.getId()));
	    ksconf.setOption(TimedRuleExecutionOption.YES);
	    ksconf.setOption(ClockTypeOption.get(ClockType.PSEUDO_CLOCK.getId()));
	    return ksconf;
	  }

}
