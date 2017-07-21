import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class A4StateMachine extends StateMachine {
    private Map<String, Long> map = new ConcurrentHashMap<>();

    protected void configure(StateMachineExecutor executor) {
		executor.register(FAICommand.class, this::fai);
		executor.register(GetQuery.class, this::get);
		executor.register(FADCommand.class, this::fad);
    }

    private Long fai(Commit<FAICommand> commit) {
		try {
			String key = commit.operation()._key;
			long oldValue = map.getOrDefault(key, 0L);
			map.put(key, oldValue + 1);
			return oldValue;
		} finally {
			commit.close();
		}
    }

    private Long fad(Commit<FADCommand> commit) {
    	try {
    		String key = commit.operation()._key;
    		long oldValue = map.getOrDefault(key, 0L);
    		map.put(key, oldValue - 1);
    		return oldValue;
		} finally {
    		commit.close();
		}
	}

    public Long get(Commit<GetQuery> commit) {
		try {
			return map.getOrDefault(commit.operation()._key, 0L);
		} finally {
			commit.release();
		}
    }
}
