import io.atomix.copycat.client.CopycatClient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CommandBuffer {
    private static Map<String, AtomicLong> commands = new ConcurrentHashMap<>();
    private static Map<String, AtomicLong> cache = new ConcurrentHashMap<>();


    public static void addIncrementCommand(String key) {
        commands.putIfAbsent(key, new AtomicLong(0));
        commands.get(key).getAndIncrement();
    }

    public static void addDecrementCommand(String key) {
        commands.putIfAbsent(key, new AtomicLong(0));
        commands.get(key).getAndDecrement();
    }

    public static long getRetVal(String key) {
        long bufferVal = commands.getOrDefault(key, new AtomicLong(0)).get();
        long cacheVal = cache.getOrDefault(key, new AtomicLong(0)).get();

        return cacheVal + bufferVal;
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
    public static void commit(CopycatClient client, ReentrantReadWriteLock lock) {
        if (commands.size() == 0) {
            return;
        }

        lock.writeLock().lock();

        // System.out.println("Submiting " + commands.size() + " commands to CopyCat");
        cache = client.submit(new BatchCommand(commands)).join();
        commands.clear();

        lock.writeLock().unlock();
    }
}
