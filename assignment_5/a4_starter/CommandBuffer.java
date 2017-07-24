import io.atomix.copycat.client.CopycatClient;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CommandBuffer {
    private static Map<String, AtomicInteger> commands = new ConcurrentHashMap<>();
    private static int OP_MAX = 8;

    private static AtomicInteger opCount = new AtomicInteger(0);
    public static STATE state = STATE.BATCHING;

    enum STATE {
        COMITTING,
        BATCHING
    }

    public static long addIncrementCommand(String key) {
        AtomicInteger old = commands.getOrDefault(key, new AtomicInteger(0));
        old.addAndGet(1);
        commands.put(key, old);
        opCount.getAndIncrement();

        return (long) old.get();
    }

    public static long addDecrementCommand(String key) {
        AtomicInteger old = commands.getOrDefault(key, new AtomicInteger(0));
        old.addAndGet(-1);
        commands.put(key, old);
        opCount.getAndIncrement();

        return (long) old.get();
    }

    /**
     * Checks the number of operations processed by this Buffer
     * @param client
     */
    public static void commitIfNeeded(CopycatClient client) {
        if (opCount.get() < OP_MAX) {
            return;
        }

        commit(client);
    }


    /**
     * The {@code commands} map keeps a map of keys and their associated deltas to be applied
     * to copy cat. This function will submit one BatchCommand to copycat with the commands
     * object as an argument
     *
     * After this function is called, we clear the {@code commands}. This is important
     * because {@code commands} keep a record of deltas, not final values. We don't to apply
     * the same delta to a key twice
     *
     * @param client - the copycat client
     */
    public static void commit(CopycatClient client) {
        // return if nothing is stored in the batch. Submitting BatchCommand to CopyCat
        // can be very slow
        if (opCount.get() == 0) {
            return;
        }

        state = STATE.COMITTING;

        Map<String, AtomicInteger> copiedMap = new HashMap<>(commands);
        commands.clear();

        System.out.println("Submiting " + copiedMap.size() + " commands to CopyCat");
        client.submit(new BatchCommand(copiedMap)).join();

        opCount = new AtomicInteger(0);
        state = STATE.BATCHING;
    }
}
