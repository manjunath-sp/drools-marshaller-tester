package drools.tester.drools.marshallng.tester;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.drools.core.ClockType;
import org.drools.core.time.SessionPseudoClock;
import org.junit.Assert;
import org.junit.Before;
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
import org.kie.api.marshalling.ObjectMarshallingStrategy;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.conf.ClockTypeOption;
import org.kie.api.runtime.rule.EntryPoint;
import org.kie.internal.marshalling.MarshallerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	public void test() throws InterruptedException, ExecutionException, TimeoutException {
		KieSession kSession = createNewkieSession();
		EntryPoint stream = kSession.getEntryPoint("EP");
		SessionPseudoClock clock = kSession.getSessionClock();

		ExecutorService thread = Executors.newSingleThreadExecutor();
		@SuppressWarnings("rawtypes")
		final Future fireUntilHaltResult = thread.submit(new Runnable() {
			@Override
			public void run() {
				kSession.fireUntilHalt();
			}
		});

		try {

			for (int i = 0; i < 1000; i++) {
				Number number = i;
				stream.insert(number);
				clock.advanceTime(2, TimeUnit.MILLISECONDS);
				backupKieSession(kSession);
			}

		} finally {
			kSession.halt();
			// wait for the engine to finish and throw exception if any was
			// thrown
			// in engine's thread
			fireUntilHaltResult.get(600, TimeUnit.SECONDS);
			thread.shutdown();
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
					marshaller = MarshallerFactory.newMarshaller(kBase,
							new ObjectMarshallingStrategy[] { MarshallerFactory.newSerializeMarshallingStrategy() });
				}
			} catch (RuntimeException e) {
				LOGGER.warn("There was an error trying to access the KieBase for modelName:", e);
			}
		}
		return marshaller;
	}

	/**
	 * Backs up the kie session in Redis
	 *
	 */
	private void backupKieSession(KieSession kSession) {

		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();) {
			getMarshaller().marshall(baos, kSession);

			// Save the session to redis
			// cacheStorageService.saveObject(K_SESSION + sessionKey,
			// baos.toByteArray());

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

		KieSession kieSession = kieContainer.newKieSession(SESSION_NAME);

		// Use for debugging : When rule match created or deleted when RHS is
		// actually fired
		// kieSession.addEventListener(new DebugAgendaEventListener());
		// Use for debugging : Notified when fact is inserted, update or
		// removed.
		// kieSession.addEventListener(new DebugRuleRuntimeEventListener());
		LOGGER.info("Created new kieSession");
		return kieSession;
	}

	/**
	 * Utility class providing methods for coping with timing issues, such as
	 * {@link java.lang.Thread#sleep(long, int)} inaccuracy, on certain OS.
	 * <p/>
	 * Inspired by
	 * http://stackoverflow.com/questions/824110/accurate-sleep-for-java-on-
	 * windows and
	 * http://andy-malakov.blogspot.cz/2010/06/alternative-to-threadsleep.html.
	 */
	public static class TimerUtils {

		private static final long SLEEP_PRECISION = Long.valueOf(System.getProperty("TIMER_SLEEP_PRECISION", "50000"));

		private static final long SPIN_YIELD_PRECISION = Long
				.valueOf(System.getProperty("TIMER_YIELD_PRECISION", "30000"));

		private TimerUtils() {
		}

		/**
		 * Sleeps for specified amount of time in milliseconds.
		 *
		 * @param duration
		 *            the amount of milliseconds to wait
		 * @throws InterruptedException
		 *             if the current thread gets interrupted
		 */
		public static void sleepMillis(final long duration) throws InterruptedException {
			sleepNanos(TimeUnit.MILLISECONDS.toNanos(duration));
		}

		/**
		 * Sleeps for specified amount of time in nanoseconds.
		 *
		 * @param nanoDuration
		 *            the amount of nanoseconds to wait
		 * @throws InterruptedException
		 *             if the current thread gets interrupted
		 */
		public static void sleepNanos(final long nanoDuration) throws InterruptedException {
			final long end = System.nanoTime() + nanoDuration;
			long timeLeft = nanoDuration;
			do {
				if (timeLeft > SLEEP_PRECISION) {
					Thread.sleep(1);
				} else if (timeLeft > SPIN_YIELD_PRECISION) {
					Thread.yield();
				}
				timeLeft = end - System.nanoTime();
			} while (timeLeft > 0);
		}
	}
}
