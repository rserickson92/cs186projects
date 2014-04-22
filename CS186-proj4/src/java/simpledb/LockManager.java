package simpledb;

import java.util.*;

public class LockManager {
    enum LockType { S, X }
    private HashMap<PageId, HashMap<TransactionId, LockType>> locks_on_page =
            new HashMap<PageId, HashMap<TransactionId, LockType>>();
    public boolean addSharedLock(PageId pid, TransactionId tid) {
        if(locks_on_page.containsKey(pid) && 
           locks_on_page.get(pid).containsValue(LockType.X)) {
            return false;
        }
        addLock(pid, tid, LockType.S);
        return true;
    }
    public boolean addExclusiveLock(PageId pid, TransactionId tid) {
        HashMap<TransactionId, LockType> locks_by_tid = locks_on_page.get(pid);
        if(locks_on_page.containsKey(pid) &&
           !locks_by_tid.isEmpty() &&
           !(locks_by_tid.size() == 1)) { //upgradeable or already has X-lock
            return false;
        }
        addLock(pid, tid, LockType.X);
        return true;
    }
    public boolean isLocked(PageId pid, TransactionId tid) {
        return locks_on_page.containsKey(pid) && 
               locks_on_page.get(pid).containsKey(tid);
    }
    public void unlock(PageId pid, TransactionId tid) {
        if(locks_on_page.containsKey(pid)) {
            locks_on_page.get(pid).remove(tid);
        }
    }
    private void addLock(PageId pid, TransactionId tid, LockType type) {
        if(!locks_on_page.containsKey(pid)) { 
            locks_on_page.put(pid, new HashMap<TransactionId, LockType>());
        }
        locks_on_page.get(pid).put(tid, type);
    }
}
