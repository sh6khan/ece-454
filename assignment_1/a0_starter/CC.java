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
         for (int i = 0; i < data.length; i++){
            if (data[i] == SPACE) { 
                number_1 = number;
                number = 0;                
            }
            else if (data[i] == NEWLINE) {
                //do union find with number_1, number
                number = 0;  
            }
            else {
                number = number * 10 + (data[i] - 48);

            }
        }


        System.out.println("size = " + data.length);
        System.out.println("max = " + max);

    }

    static class FileReader implements Runnable {
        public FileReader() {

        }
        public void run() {
            System.out.println("doing random tings");
        }
    }

    static public class Tuple{
        int first;
        int second;

        public Tuple(int first, int second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + first;
            result = prime * result + second;
            return result;
        }

        @Override
        public boolean equals(Object obj){
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Tuple other = (Tuple) obj;
            if (first != other.first || second != other.second)
                return false;
            return true;
        }
    }
}
