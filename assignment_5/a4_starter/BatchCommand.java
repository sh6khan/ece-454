import io.atomix.copycat.Command;

import java.util.Set;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchCommand implements Command {
    Set<Map.Entry<String, AtomicInteger>> _changes;

    public BatchCommand(Set<Map.Entry<String, AtomicInteger>> changes) {
        _changes = changes;
    }
}
