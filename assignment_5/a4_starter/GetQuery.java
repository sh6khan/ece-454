import io.atomix.copycat.Query;

public class GetQuery implements Query<Long> {
    String _key;

    public GetQuery(String key) {
        _key = key;
    }

    public ConsistencyLevel consistency() {
	return ConsistencyLevel.LINEARIZABLE_LEASE;
    }
}
