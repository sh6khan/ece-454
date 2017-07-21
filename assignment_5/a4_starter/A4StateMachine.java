import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;


public class A4StateMachine extends StateMachine {
    private long myValue = 0;

    protected void configure(StateMachineExecutor executor) {
	executor.register(FAICommand.class, this::fai);
	executor.register(GetQuery.class, this::get);
    }

    private Long fai(Commit<FAICommand> commit) {
	try {
	    Long old = myValue;
	    myValue++;
	    return old;
	} finally {
	    commit.close();
	}
    }

    public Long get(Commit<GetQuery> commit) {
	try {
	    return myValue;
	} finally {
	    commit.release();
	}
    }
}
