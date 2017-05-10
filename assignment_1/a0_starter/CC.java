import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;




public class CC {
    private static byte[] data;
    public static final int SPACE = 32;
    public static final int NEWLINE = 10;


    public static void main(String[] args) throws Exception {
        int numCores = Integer.parseInt(args[0]);
        int maxVertex = 0;

        int numthreads = 4;
        HashMap<Integer, Integer> map = new HashMap<>();
        Path path = Paths.get(args[1]);
        data = Files.readAllBytes(path);
        int number_1 = 0;
        int number = 0;
        int max = 0;
        for (int i = 0; i < data.length; i++){
            if (data[i] == SPACE || data[i] == NEWLINE) {
                if (number > max){
                    max = number;
               }
               number = 0;
            }
            else {
                number = number * 10 + (data[i] - 48);

            }
        }

        int[] biject = new int[max+1];
        UnionFind uf = new UnionFind(max + 1);


         for (int i = 0; i < data.length; i++){
            if (data[i] == SPACE) { 
                number_1 = number;
                number = 0;                
            }
            else if (data[i] == NEWLINE) {
                uf.union(number_1, number);
                number = 0;  
            }
            else {
                number = number * 10 + (data[i] - 48);

            }
        }

        try {
            uf.printToFile(args[2]);
        } catch (Exception e) {
            System.out.println("failed to write to file");
             // do nothing;
        }
    }

    /**
     * UnionFind datastructure based on the following paper
     * https://www.cs.princeton.edu/~rs/AlgsDS07/01UnionFind.pdf
     */
    static public class UnionFind {

        private int[] componenentSizeMap;
        public int[] mapping;
        private boolean[] exists;

        public UnionFind(int maxNode) {
            mapping = new int[maxNode];
            exists = new boolean[maxNode];
            componenentSizeMap = new int[maxNode];

            for (int i = 0; i < maxNode; i++) {
                mapping[i] = i;
                componenentSizeMap[i] = 1;
            }
        }

        public int find(int node) {
            // Find the root of the component
            // which is defined when root == mapping[root]
            int root = node;
            while( root != mapping[root] ) {
                root = mapping[root];
            }

            mapping[node] = root;

            return root;
        }

        /**
         * Connect the root nodes of two seperate components
         */
        public void union(int nodeA, int nodeB) {
            int nodeARoot = find(nodeA);
            int nodeBRoot = find(nodeB);

            if (nodeARoot == nodeBRoot) {
                return;
            }

            if (componenentSizeMap[nodeARoot] < componenentSizeMap[nodeBRoot]) {
                mapping[nodeARoot] = nodeBRoot;
                componenentSizeMap[nodeBRoot] += componenentSizeMap[nodeARoot];
            } else {
                mapping[nodeBRoot] = nodeARoot;
                componenentSizeMap[nodeARoot] += componenentSizeMap[nodeBRoot];
            }


            exists[nodeA] = true;
            exists[nodeB] = true;
        }

        public void printToFile(String fileName) throws IOException {
            PrintWriter pw = new PrintWriter(new FileWriter(fileName));
            for (int i = 0; i < mapping.length; i++) {
                if (exists[i] != true) {
                    continue;
                }

                pw.println(i + " " + find(i));
            }

            pw.close();
        }
    }
}
