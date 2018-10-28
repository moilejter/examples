package net.developersnotebook.examples.parallel;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class ParallelTestHarnessImpl implements ParallelTestHarness {
	private static final Logger logger = LogManager.getLogger();
	
	@Autowired PlatformTransactionManager txnManager;
	@Autowired(required=false) TransactionTemplate txnTemplate;
	@Autowired(required=false) @Qualifier("parallelTestHarnessExecutor") ExecutorService executorSvc;
	
	@PostConstruct 
	private void setup() {
		if (executorSvc == null) {
			int numThreads = (Runtime.getRuntime().availableProcessors() + 1) / 2;
			executorSvc = Executors.newWorkStealingPool(numThreads);
		}
		if (txnTemplate == null) {
			txnTemplate = new TransactionTemplate(txnManager);
		}
	}

	@Override
	public void execute(Runnable assertionCallback, TransactionCallback<?>... txnCallbacks) {
		if ((txnCallbacks == null) || (txnCallbacks.length == 0)) {
			if (assertionCallback != null) assertionCallback.run();
		} else {
			executeInParallel(assertionCallback, txnCallbacks);
		}
	}

	private void executeInParallel(Runnable assertionCallback, TransactionCallback[] txnCallbacks) {
		CyclicBarrier barrier = new CyclicBarrier(txnCallbacks.length, assertionCallback);
		
		for (TransactionCallback<?> txnCallback : txnCallbacks) {
			TransactionCallbackWithoutResult wrapper =
				new TransactionCallbackWithoutResult() {
					@Override
					protected void doInTransactionWithoutResult(TransactionStatus status) {
						txnCallback.doInTransaction(status);
						try {
							barrier.await();
						} catch (InterruptedException | BrokenBarrierException e) {
							logger.warn("txnCallback aborted: " + e, e);
						} finally {
							status.setRollbackOnly();
						}
					}
			};
			Runnable handler = 
				new Runnable() {
					public void run() {
						txnTemplate.execute(wrapper);
					}
			};
			executorSvc.execute(handler);
		}
	}

}
