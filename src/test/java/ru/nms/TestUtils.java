package ru.nms;

import org.apache.commons.math3.linear.RealVector;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

import static ru.nms.TestDataGenerator.generateNVectors;
import static ru.nms.utils.BallTreeUtils.*;
import static ru.nms.utils.Constants.BALL_TREE_HEADER;
import static ru.nms.utils.Constants.TEMP_DIR;

public class TestUtils {

    public static final Random RANDOM = new Random(0);

    private static int CHUNK_SIZE = Integer.MAX_VALUE;
    public static String SOURCE_FILE_DIR = "source_files\\";

    public static void writeVectorsToFile(List<RealVector> vectors, String fullPath) throws IOException {
        if (vectors.isEmpty()) {
            throw new IllegalArgumentException("The list of vectors is empty.");
        }

        int dimension = vectors.get(0).getDimension();
        long vectorCount = vectors.size();

        try (RandomAccessFile file = new RandomAccessFile(fullPath, "rw");
             FileChannel fileChannel = file.getChannel()) {
            fileChannel.write(ByteBuffer.wrap(BALL_TREE_HEADER.getBytes(StandardCharsets.UTF_8)));

            file.writeLong(vectorCount);
            file.writeInt(dimension);
            setPositionToStartOfVectorData(fileChannel, dimension);

            for (RealVector vector : vectors) {
                putVectorToFile(file, vector);
            }
        }
    }

    private static void generateFileWithNVectors(String fullPath, long n, int dimension) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(fullPath, "rw");
             FileChannel fileChannel = file.getChannel()) {
            fileChannel.write(ByteBuffer.wrap(BALL_TREE_HEADER.getBytes(StandardCharsets.UTF_8)));

            file.writeLong(n);
            file.writeInt(dimension);
            setPositionToStartOfVectorData(fileChannel, dimension);

            while (n > 0) {
                int chunkVectorsAmount = Math.toIntExact(Math.min(n, CHUNK_SIZE / getVectorSize(dimension)));
                List<RealVector> vectors = generateNVectors(RANDOM, chunkVectorsAmount, dimension);
                for (RealVector vector : vectors) {
                    putVectorToFile(file, vector);
                }
                n -= chunkVectorsAmount;
            }
        }
    }

    public static void cleanTempDir() {
        File directory = new File(TEMP_DIR);
        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles();

            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {
                        if (!file.delete()) {
                            System.out.println("Failed to delete: " + file.getName());
                        }
                    }
                }
            } else {
                System.out.println("The directory is empty or could not be read.");
            }
        } else {
            System.out.println("The specified path is not a directory or does not exist.");
        }
    }

    public static void main(String[] args) throws IOException {
        long n = 10000000;
        int dimension = 256;
        generateFileWithNVectors(constructSourceFilePath(n, dimension), n, dimension);
    }

    public static String constructSourceFilePath(long n, int dimension) {
        return SOURCE_FILE_DIR + n + "_" + dimension;
    }
}
