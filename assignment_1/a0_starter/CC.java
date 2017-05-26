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
        Path path = Paths.get(args[1]);
        data = Files.readAllBytes(path);
        int edgeCount = 0;
        for (int i = 0; i < data.length; i++){
            if (data[i] == NEWLINE) {
                edgeCount++;
            }
        }

        // there can be a max of 2 * the number of edges
        int nodeCount = edgeCount * 2;
        int[] allNodes = new int[nodeCount];
        int index = 0;


        int number_1 = 0;
        int number = 0;
         for (int i = 0; i < data.length; i++){
            if (data[i] == SPACE) {
                number_1 = number;
                number = 0;
            }
            else if (data[i] == NEWLINE) {
                allNodes[index++] = number_1;
                allNodes[index++] = number;
                number = 0;
            }
            else {
                number = number * 10 + (data[i] - 48);

            }
        }

        UnionFind uf = new UnionFind(allNodes);


        for (int i = 0; i < allNodes.length; i++) {
            uf.union(allNodes[i++], allNodes[i]);
        }


        try {
            uf.printToFile(args[2], allNodes);
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

        private int[] _componenentSizeMap;
        public int[] _mappings;
        public int[] _reverse_bijection;
        private HashMap<Integer, Integer> _bijection = new HashMap<>();



        public UnionFind(int[] allNodes) {
            int id = 0;
            _reverse_bijection = new int[allNodes.length];

            for (int node : allNodes) {
                if (!_bijection.containsKey(node)) {
                    _bijection.put(node, id);
                    _reverse_bijection[id] = node;
                    id++;
                }
            }

            int size = _bijection.size();
            System.out.println(size);

            _mappings = new int[size];
            _componenentSizeMap = new int[size];

            for (int i = 0; i < size; i++) {
                _mappings[i] = i;
                _componenentSizeMap[i] = 1;
            }

        }

        public int find(int node) {
            // Find the root of the component
            // which is defined when root == mapping[root]
            int root = node;
            while( root != _mappings[root] ) {
                root = _mappings[root];
            }

            while(node != root) {
                int next = _mappings[node];
                _mappings[node] = root;
                node = next;
            }

            return root;
        }

        /**
         * Connect the root nodes of two seperate components
         */
        public void union(int a, int b) {
            int nodeA = _bijection.get(a);
            int nodeB = _bijection.get(b);

            int nodeARoot = find(nodeA);
            int nodeBRoot = find(nodeB);

            if (nodeARoot == nodeBRoot) {
                return;
            }
            

            if (_componenentSizeMap[nodeARoot] < _componenentSizeMap[nodeBRoot]) {
                _mappings[nodeARoot] = nodeBRoot;
                _componenentSizeMap[nodeBRoot] += _componenentSizeMap[nodeARoot];
            } else {
                _mappings[nodeBRoot] = nodeARoot;
                _componenentSizeMap[nodeARoot] += _componenentSizeMap[nodeBRoot];
            }

        }

        public void printToFile(String fileName, int[] allNodes) throws IOException {
            PrintWriter pw = new PrintWriter(new FileWriter(fileName));
            for (int i : _bijection.keySet()) {
                pw.println(i + " " + _reverse_bijection[find(_bijection.get(i))]);
            }

            pw.close();
        }
    }
}
