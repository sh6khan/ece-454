import java.util.*;
import java.util.concurrent.*;
import java.io.*;

public class CC {
    public static void main(String[] args) throws Exception {
        int numCores = Integer.parseInt(args[0]);
        int maxVertex = 0;

        // read graph from input file
        Set<Integer> vertices = new HashSet<>();
        FileInputStream fis = new FileInputStream(args[1]);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        String line = null;
        while ((line = br.readLine()) != null) {
            String[] verts = line.split("\\s+");
            int v1 = Integer.parseInt(verts[0]);
            int v2 = Integer.parseInt(verts[1]);
            maxVertex = Math.max(maxVertex, v1);
            maxVertex = Math.max(maxVertex, v2);
            vertices.add(v1);
            vertices.add(v2);
        }

        br.close();

        // start worker threads
        Thread[] threads = new Thread[numCores];
        for (int t = 0; t < numCores; t++) {
            threads[t] = new Thread(new MyRunnable());
            threads[t].start();
        }

        // wait for threads to finish
        for (int t = 0; t < numCores; t++) {
            threads[t].join();
        }

        // generate output
        PrintWriter pw = new PrintWriter(new FileWriter(args[2]));
        for (int i: vertices) {
            pw.println(i + " " + "<component label goes here>");
        }

        // close the writer
        pw.close();
    }

    static class MyRunnable implements Runnable {
        public void run() {
            System.out.println("doing random tings");
        }
    }
}
