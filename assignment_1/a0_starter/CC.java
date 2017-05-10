import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;


public class CC {
    String delims = "\n";
    int SPACE = 50;
    int NEWLINE = 10;


    public static void main(String[] args) throws Exception {
        int numCores = Integer.parseInt(args[0]);
        int maxVertex = 0;

        int numthreads = 4;
        // ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        // ConcurrentMap<Integer, Integer> map = new ConcurrentHashMap<>();
        HashMap<Integer, Integer> map = new HashMap<>();
        Path path = Paths.get(args[1]);
        byte[] data = Files.readAllBytes(path);
        int number_1 = 0;
        int number = 0;
        for (int i = 0; i < data.length; i++){
            if (data[i] == 32) {
                number_1 = number;
                number = 0;                
            }
            else if (data[i] == 10) {
                //hash number_1, number
                System.out.println("" + number_1 + " " + number);
                number = 0;  
            }
            else {
                number = number * 10 + (data[i] - 48);

            }

        }
        System.out.println("size = " + data.length);
        // // start worker threads
        // Thread[] threads = new Thread[numCores];
        // for (int t = 0; t < numCores; t++) {
        //     threads[t] = new Thread(new MyRunnable());
        //     threads[t].start();
        // }

        // // wait for threads to finish
        // for (int t = 0; t < numCores; t++) {
        //     threads[t].join();
        // }

        // // generate output
        // PrintWriter pw = new PrintWriter(new FileWriter(args[2]));
        // for (int i: vertices) {
        //     pw.println(i + " " + "<component label goes here>");
        // }

        // // close the writer
        // pw.close();
    }

    static class MyRunnable implements Runnable {
        public void run() {
            System.out.println("doing random tings");
        }
    }
}
