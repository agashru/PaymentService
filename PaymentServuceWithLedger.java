package org.example.PaymentService;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class PaymentServuceWithLedger {
    private static final long LOCK_TIMEOUT_MS = 500;

    // 1. THE LEDGER: AccountID -> Balance (The State)
    private final Map<String, BigDecimal> ledger = new ConcurrentHashMap<>();

    // 2. THE LOCKS: AccountID -> Lock (Lock Striping)
    private final Map<String, ReentrantLock> accountLocks = new ConcurrentHashMap<>();

    // 3. IDEMPOTENCY: RequestID -> Result
    private final Map<String, String> idempotencyStore = new ConcurrentHashMap<>();

    // 4. AUDIT TRAIL: Full History (Immutable)
    public record TransactionRecord(String txId, String from, String to, BigDecimal amount, Instant ts) {
    }

    private final List<TransactionRecord> auditTrail = Collections.synchronizedList(new ArrayList<>());

    public void createAccount(String id, BigDecimal initialBalance) {
        ledger.putIfAbsent(id, initialBalance);
        accountLocks.putIfAbsent(id, new ReentrantLock(true));
    }

    public String transfer(String requestId, String fromId, String toId, BigDecimal amount) throws InterruptedException {

        // --- STEP 1: IDEMPOTENCY GUARD ---
        // We "reserve" the requestId immediately to prevent race conditions
        String inProgress = "PROCESSING_" + requestId;
        String existing = idempotencyStore.putIfAbsent(requestId, inProgress);
        if (existing != null) return existing;

        try {
            // Validation
            if (fromId.equals(toId)) throw new IllegalArgumentException("Self-transfer forbidden");
            if (amount.compareTo(BigDecimal.ZERO) <= 0) throw new IllegalArgumentException("Amount must be positive");

            ReentrantLock firstLock = getLock(fromId, toId, true);
            ReentrantLock secondLock = getLock(fromId, toId, false);

            // --- STEP 2: ATOMIC PROCESSING ---
            if (firstLock.tryLock(LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                try {
                    if (secondLock.tryLock(LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                        try {
                            // Fetch balances from Ledger
                            BigDecimal sourceBal = ledger.get(fromId);
                            BigDecimal destBal = ledger.get(toId);

                            if (sourceBal == null || destBal == null)
                                throw new NoSuchElementException("Account missing");
                            if (sourceBal.compareTo(amount) < 0) throw new IllegalStateException("Insufficient funds");

                            // EXECUTE: Update Ledger
                            String txId = "TXN_" + UUID.randomUUID();
                            ledger.put(fromId, sourceBal.subtract(amount));
                            ledger.put(toId, destBal.add(amount));

                            // APPEND: Audit Trail
                            auditTrail.add(new TransactionRecord(txId, fromId, toId, amount, Instant.now()));

                            // COMMIT: Finalize Idempotency
                            String result = "SUCCESS_" + txId;
                            idempotencyStore.put(requestId, result);
                            return result;

                        } finally {
                            secondLock.unlock();
                        }
                    }
                } finally {
                    firstLock.unlock();
                }
            }
            throw new RuntimeException("System busy: Lock timeout");

        } catch (Exception e) {
            // Clean up idempotency if a validation/system error occurred so client can retry
            idempotencyStore.remove(requestId);
            throw e;
        }
    }

    private ReentrantLock getLock(String id1, String id2, boolean returnFirst) {
        String firstId = id1.compareTo(id2) < 0 ? id1 : id2;
        String secondId = id1.compareTo(id2) < 0 ? id2 : id1;
        return accountLocks.get(returnFirst ? firstId : secondId);
    }

    public BigDecimal getBalance(String id) {
        return ledger.getOrDefault(id, BigDecimal.ZERO);
    }
}
