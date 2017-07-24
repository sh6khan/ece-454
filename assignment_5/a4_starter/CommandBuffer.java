import io.atomix.copycat.client.CopycatClient;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CommandBuffer {
    private static Map<String, AtomicInteger> commands = new ConcurrentHashMap<>();

    public static long addIncrementCommand(String key) {
        AtomicInteger old = commands.getOrDefault(key, new AtomicInteger(0));
        old.addAndGet(1);
        commands.put(key, old);

        return (long)old.get();
    }

    public static void addDecrementCommand(String key) {
        if (!commands.containsKey(key)){
            commands.put(key, new AtomicInteger(0));
        }

        commands.get(key).addAndGet(-1);
    }


    /**
     * getter for the key<->delta currently being batched
     *
     * @param key - the key from from the commands map
     * @return delta
     */
    public static Long getDelta(String key) {
        return (long)commands.get(key).get();
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
        if (commands.size() == 0) {
            return;
        }

        // create a copy of the entire map as an entry set
        Set<Map.Entry<String, AtomicInteger>> entrySet = commands.entrySet();

        System.out.println("Submiting " + entrySet.size() + " commands to CopyCat");
        client.submit(new BatchCommand(entrySet)).join();

        // clear the map for future commits
        commands.clear();
    }
}
