package org.example.PaymentService;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class PaymentService {
    private static final long LOCK_TIMEOUT_MS = 500; // 500ms limit
    // Storage for processed requests: Key = RequestID, Value = Previous Result
    // In production, this would be a Redis cache with a TTL (Time-To-Live)
    private final Map<String, String> processedRequests = new ConcurrentHashMap<>();

    private static class Account {
        private BigDecimal balance = BigDecimal.ZERO;
        private final ReentrantLock lock = new ReentrantLock(true); // 'true' for fairness
    }

    // Represents a single immutable entry in the ledger
    public record Transaction(String id, String fromAccount, String toAccount, BigDecimal money, Instant timestamp) {}

    private final Map<String, Account> accounts = new HashMap<>();
    private final List<Transaction> auditTrail = Collections.synchronizedList(new ArrayList<>());

    public String transfer(String requestId, String fromId, String toId, BigDecimal amount) throws InterruptedException {
        // 1. IDEMPOTENCY CHECK
        // If we've seen this ID before, return the cached result immediately.
        if (processedRequests.containsKey(requestId)) {
            return processedRequests.get(requestId);
        }

        if (fromId.equals(toId)) throw new IllegalArgumentException("Self-transfer not allowed");

        Account source = accounts.get(fromId);
        Account destination = accounts.get(toId);

        // Sort locks to prevent deadlock
        Account first = fromId.compareTo(toId) < 0 ? source : destination;
        Account second = fromId.compareTo(toId) < 0 ? destination : source;

        // Attempt to acquire the first lock
        if (first.lock.tryLock(LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            try {
                // Attempt to acquire the second lock
                if (second.lock.tryLock(LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    try {
                        // 4. DOUBLE-CHECK IDEMPOTENCY
                        // Check again inside the lock to prevent a race condition
                        // where two threads with the same requestId enter at once.
                        if (processedRequests.containsKey(requestId)) {
                            return processedRequests.get(requestId);
                        }

                        // CRITICAL SECTION START
                        if (source.balance.compareTo(amount) < 0) {
                            throw new IllegalStateException("Insufficient funds");
                        }

                        source.balance = source.balance.subtract(amount);
                        destination.balance = destination.balance.add(amount);

                        String transactionId = "TXN_" + UUID.randomUUID();
                        processedRequests.put(requestId, transactionId);

                        // 3. Audit Trail
                        Transaction tx = new Transaction(transactionId, fromId, toId, amount, Instant.now());
                        auditTrail.add(tx);

                        return "SUCCESS_" + UUID.randomUUID();
                        // CRITICAL SECTION END
                    } finally {
                        second.lock.unlock();
                    }
                } else {
                    // Fail fast if second lock is busy
                    throw new RuntimeException("System busy: Could not acquire second account lock");
                }
            } finally {
                first.lock.unlock();
            }
        } else {
            // Fail fast if first lock is busy
            throw new RuntimeException("System busy: Could not acquire primary account lock");
        }
    }
}
