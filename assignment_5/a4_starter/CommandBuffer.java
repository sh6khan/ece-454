import io.atomix.copycat.client.CopycatClient;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class CommandBuffer {
    private static Map<String, AtomicLong> commands = new ConcurrentHashMap<>();
    public static Instant lastCommitTime = Instant.now();

    public static STATE state = STATE.BATCHING;

    enum STATE {
        COMITTING,
        BATCHING
    }

    public static long addIncrementCommand(String key) {
        if (state.equals(STATE.COMITTING)) {
            // System.out.println("addIncrementCommand is submiting while clearing is happening");
        }

        commands.putIfAbsent(key, new AtomicLong(0));
        commands.get(key).getAndIncrement();

        return 0L;
    }

    public static long addDecrementCommand(String key) {
        if (state.equals(STATE.COMITTING)) {
            // System.out.println("addDecrementCommand is submiting while clearing is happening");
        }

        commands.putIfAbsent(key, new AtomicLong(0));
        commands.get(key).getAndDecrement();

        return 0L;
    }


    private static Map<String, AtomicLong> getCommands() {
        return new ConcurrentHashMap<>(commands);
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
        state = STATE.COMITTING;

        Map<String, AtomicLong> copiedMap = new ConcurrentHashMap<>(commands);
        // commands.clear();

        System.out.println("key-0 >> " + commands.get("key-0"));

        // System.out.println("Submiting " + copiedMap.size() + " commands to CopyCat");
        client.submit(new BatchCommand(copiedMap)).join();
        lastCommitTime = Instant.now();

        state = STATE.BATCHING;
    }
}
