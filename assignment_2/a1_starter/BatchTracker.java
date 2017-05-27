import java.time.Duration;
import java.time.Instant;

/**
 * This class is used by the BENode in order to keep track of when the last batch
 * was sent from the FENode.
 *
 * If its been a long time since the BENode has received a packet, then that implies that
 * the FENode has been killed
 *
 * Each BENode creates and maintains an instance of this class
 */

public class BatchTracker {
    private static Instant lastBatchTime = Instant.now();

    // how long we wait for batch to determine if the FENode is down
    public static final Duration TIMEOUT = Duration.ofSeconds(10);


    public static void receivedBatch() {
        lastBatchTime = Instant.now();
    }

    public static boolean isFENodeDown() {
        System.out.println("checking the last time we got a batch");
        Duration timeSinceLastBatch = Duration.between(lastBatchTime, Instant.now());
        return timeSinceLastBatch.compareTo(TIMEOUT) > 0;

    }
}
