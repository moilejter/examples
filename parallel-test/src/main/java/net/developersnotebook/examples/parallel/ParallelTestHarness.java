package net.developersnotebook.examples.parallel;

import org.springframework.transaction.support.TransactionCallback;

public interface ParallelTestHarness {

	void execute(Runnable assertionCallback, TransactionCallback<?> ... txnCallbacks);
}
