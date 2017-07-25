import io.atomix.copycat.Command;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class BatchCommand implements Command<Map<String, AtomicLong>> {
    Map<String, AtomicLong> _changes;

    public BatchCommand(Map<String, AtomicLong> changes) {
        _changes = changes;
    }
}
