import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


public class A4StateMachine extends StateMachine {
    private Map<String, AtomicLong> map = new ConcurrentHashMap<>();

    protected void configure(StateMachineExecutor executor) {
		executor.register(FAICommand.class, this::fai);
		executor.register(GetQuery.class, this::get);
		executor.register(FADCommand.class, this::fad);
		executor.register(BatchCommand.class, this::batchCommit);
    }

    public Map<String, AtomicLong> batchCommit(Commit<BatchCommand> commit) {
    	try {
    		for (Map.Entry<String, AtomicLong> entry: commit.operation()._changes.entrySet()) {
    			map.putIfAbsent(entry.getKey(), new AtomicLong(0));
    			map.get(entry.getKey()).addAndGet(entry.getValue().get());
			}

			return map;
		} finally {
			commit.close();
		}
	}

    private Long fai(Commit<FAICommand> commit) {
		try {
			String key = commit.operation()._key;
			AtomicLong val = map.getOrDefault(key, new AtomicLong(0));
			long ret = val.getAndIncrement();
			map.put(key, val);
			return ret;
		} finally {
			commit.close();
		}
    }

    private Long fad(Commit<FADCommand> commit) {
    	try {
			String key = commit.operation()._key;
			AtomicLong val = map.getOrDefault(key, new AtomicLong(0));
			long ret = val.getAndDecrement();
			map.put(key, val);
			return ret;
		} finally {
    		commit.close();
		}
	}

    public Long get(Commit<GetQuery> commit) {
		try {
			return map.getOrDefault(commit.operation()._key, new AtomicLong(0)).get();
		} finally {
			commit.release();
		}
    }
}
