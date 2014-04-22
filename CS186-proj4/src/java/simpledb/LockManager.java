package simpledb;

import java.util.*;

public class LockManager {
    enum LockType { S, X }
    class LockRequest {
        TransactionId tid;
        LockType type;
        public LockRequest(TransactionId tid, LockType type) {
            this.tid = tid;
            this.type = type;
        }
    }
    private HashMap<PageId, LinkedList<LockRequest>> locks =
            new HashMap<PageId, LinkedList<LockRequest>>();
    public synchronized boolean addSharedLock(PageId pid, TransactionId tid) {
        addRequest(pid, tid, LockType.S);
        return locks.get(pid).getFirst().type != LockType.X;
    }
    public synchronized boolean addExclusiveLock(PageId pid, TransactionId tid) {
        addRequest(pid, tid, LockType.X);
        LockRequest req = locks.get(pid).getFirst();
        return (req.tid.equals(tid) && req.type == LockType.X)
               || upgradeLock(pid, tid);
    }
    public boolean isLocked(PageId pid, TransactionId tid) {
        if(!locks.containsKey(pid)) { return false; }
        LinkedList<LockRequest> reqs = locks.get(pid);
        if(reqs.getFirst().type == LockType.S) {
            for(LockRequest req : reqs) {
                if(req.type != LockType.X && req.tid.equals(tid)) {
                    return true;
                }
            }
            return false;
        } else {
            return reqs.getFirst().tid.equals(tid);
        }
    }
    public synchronized void unlock(PageId pid, TransactionId tid) {
        if(!locks.containsKey(pid)) { return; }
        LinkedList<LockRequest> reqs = locks.get(pid);
        LockRequest unlock_me = null;
        if(reqs.getFirst().type == LockType.X && 
           !reqs.getFirst().tid.equals(tid)) { return; }
        for(LockRequest req : reqs) {
            if(req.tid.equals(tid)) {
                unlock_me = req;
                break;
            }
        }
        reqs.remove(unlock_me);
    }
    private void addRequest(PageId pid, TransactionId tid, LockType type) {
        if(!locks.containsKey(pid)) {
            locks.put(pid, new LinkedList<LockRequest>());
        } 
        locks.get(pid).add(new LockRequest(tid, type));
    }
    private boolean upgradeLock(PageId pid, TransactionId tid) {
        LinkedList<LockRequest> reqs = locks.get(pid);
        LockRequest req = reqs.getFirst();
        if(req.tid.equals(tid) && req.type == LockType.S &&
           reqs.get(1).tid.equals(tid)) {
            reqs.remove();
            return true;
        }
        return false;
    }
}
