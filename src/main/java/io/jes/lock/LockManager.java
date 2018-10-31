package io.jes.lock;

public interface LockManager {

    void doExclusive(String lockName, Runnable action);

    void doProtectedRead(String lockName, Runnable action);

    void doProtectedWrite(String lockName, Runnable action);

}
