package org.example.PaymentService;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class PaymentServiceWithLedger {
    private static final long LOCK_TIMEOUT_MS = 1000;

    private final Map<String, BigDecimal> ledger = new ConcurrentHashMap<>();
    private final Map<String, ReentrantLock> accountLocks = new ConcurrentHashMap<>();
    private final Map<String, String> idempotencyStore = new ConcurrentHashMap<>();

    // Non-blocking audit trail for high-throughput writes
    private final Queue<TransactionRecord> auditTrail = new ConcurrentLinkedQueue<>();

    public record TransactionRecord(String txId, String from, String to, BigDecimal amount, Instant ts) {}

    public void createAccount(String id, BigDecimal initialBalance) {
        ledger.putIfAbsent(id, initialBalance);
        accountLocks.putIfAbsent(id, new ReentrantLock(true));
    }

    public String transfer(String requestId, String fromId, String toId, BigDecimal amount) throws InterruptedException {
        // --- PHASE 1: EXTERNAL IDEMPOTENCY CHECK (Fast Fail) ---
        // Prevents the "Thundering Herd" from hitting the locks
        String status = idempotencyStore.get(requestId);
        if (status != null) {
            if (status.equals("PROCESSING")) throw new IllegalStateException("Transaction in progress");
            return status; // Return existing SUCCESS_TXID
        }

        // Reserve the ID
        if (idempotencyStore.putIfAbsent(requestId, "PROCESSING") != null) {
            return idempotencyStore.get(requestId);
        }

        try {
            // Validation
            if (fromId.equals(toId)) throw new IllegalArgumentException("Self-transfer forbidden");
            if (amount.compareTo(BigDecimal.ZERO) <= 0) throw new IllegalArgumentException("Positive amount required");

            // Determine Lock Order
            String firstId = fromId.compareTo(toId) < 0 ? fromId : toId;
            String secondId = fromId.compareTo(toId) < 0 ? toId : fromId;

            ReentrantLock lock1 = accountLocks.get(firstId);
            ReentrantLock lock2 = accountLocks.get(secondId);
            if (lock1 == null || lock2 == null) throw new NoSuchElementException("Account missing");

            // --- PHASE 2: ACQUIRE LOCKS ---
            if (lock1.tryLock(LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                try {
                    if (lock2.tryLock(LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                        try {
                            // --- PHASE 3: INTERNAL IDEMPOTENCY CHECK (Double-Check) ---
                            // Check again inside the critical section in case another thread
                            // finished the work while we were waiting for these locks.
                            String innerStatus = idempotencyStore.get(requestId);
                            if (innerStatus != null && !innerStatus.equals("PROCESSING")) {
                                return innerStatus;
                            }

                            // --- PHASE 4: ATOMIC EXECUTION ---
                            BigDecimal sourceBal = ledger.get(fromId);
                            BigDecimal destBal = ledger.get(toId);

                            if (sourceBal.compareTo(amount) < 0) throw new IllegalStateException("Insufficient funds");

                            String txId = "TXN_" + UUID.randomUUID();

                            // State updates
                            ledger.put(fromId, sourceBal.subtract(amount));
                            ledger.put(toId, destBal.add(amount));
                            auditTrail.add(new TransactionRecord(txId, fromId, toId, amount, Instant.now()));

                            // Commit Idempotency
                            String result = "SUCCESS_" + txId;
                            idempotencyStore.put(requestId, result);
                            return result;

                        } finally {
                            lock2.unlock();
                        }
                    }
                } finally {
                    lock1.unlock();
                }
            }
            throw new RuntimeException("System busy: Could not acquire locks");

        } catch (Exception e) {
            // If we hit an error (Insufficient funds, timeout, etc.),
            // we remove the "PROCESSING" tag so the client can actually retry.
            idempotencyStore.remove(requestId);
            throw e;
        }
    }

    public BigDecimal getBalance(String id) throws InterruptedException {
        ReentrantLock lock = accountLocks.get(id);
        if (lock == null) return BigDecimal.ZERO;

        // Reading inside the lock ensures we don't see "partial" transfers (Consistency)
        if (lock.tryLock(LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            try {
                return ledger.getOrDefault(id, BigDecimal.ZERO);
            } finally {
                lock.unlock();
            }
        }
        throw new RuntimeException("Timed out reading balance");
    }
}