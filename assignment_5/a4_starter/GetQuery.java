import io.atomix.copycat.Query;

public class GetQuery implements Query<Long> {
    public GetQuery() {
    }

    public ConsistencyLevel consistency() {
	return ConsistencyLevel.LINEARIZABLE_LEASE;
    }
}
