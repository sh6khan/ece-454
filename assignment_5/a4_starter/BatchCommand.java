import io.atomix.copycat.Command;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchCommand implements Command {
    Map<String, AtomicInteger> _changes;

    public BatchCommand(Map<String, AtomicInteger> changes) {
        _changes = changes;
    }
}
