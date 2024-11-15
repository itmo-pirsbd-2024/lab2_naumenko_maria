package ru.nms;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.linear.RealVector;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import static ru.nms.utils.BallTreeUtils.*;
import static ru.nms.utils.Constants.*;

@Slf4j
public class BallTreeBuilder {

    public static void build(String fileName, boolean rootNode) throws IOException {

        long leftChildFileName = System.nanoTime();
        long rightChildFileName = System.nanoTime();
        while (leftChildFileName == rightChildFileName) {
            rightChildFileName = System.nanoTime();
        }

        try (RandomAccessFile file = new RandomAccessFile(ROOT_DIR + fileName, "rw");
             FileChannel fileChannel = file.getChannel()) {
            verifyFileFormat(fileChannel, fileName);
            long totalVectors = file.readLong();
            int dimension = file.readInt();
            log.info("building tree for node {}, which has {} vectors with {} dimension", fileName, totalVectors, dimension);
            if (totalVectors <= NODE_MAX_SIZE) {
                setChildNodeNames(file, dimension, -1, -1);
                return;
            }
            RealVector centroid = rootNode
                    ? calculateCentroidAndSetMeta(file, dimension, totalVectors)
                    : getCentroid(file, dimension);

            RealVector farthestPoint = findFarthestVector(fileChannel, centroid, dimension, totalVectors);
            RealVector secondFarthestPoint = findSecondFarthestVector(fileChannel, centroid, farthestPoint, dimension, totalVectors);
            RealVector baseLine = farthestPoint.subtract(secondFarthestPoint);
            RealVector median = findApproximateMedianByBaseLine(fileChannel, baseLine, dimension, totalVectors, SAMPLE_SIZE);

            try (RandomAccessFile leftChildFile = new RandomAccessFile(ROOT_DIR + leftChildFileName, "rw");
                 RandomAccessFile rightChildFile = new RandomAccessFile(ROOT_DIR + rightChildFileName, "rw");
            ) {
                distributeVectorsBasedOnMedian(fileChannel, leftChildFile, rightChildFile, baseLine, median, dimension, totalVectors);
            }
            setChildNodeNames(file, dimension, leftChildFileName, rightChildFileName);
        }

        build(String.valueOf(leftChildFileName), false);
        build(String.valueOf(rightChildFileName), false);
    }

    private static RealVector calculateCentroidAndSetMeta(RandomAccessFile file, int dimension, long totalVectors) throws IOException {
        RealVector centroid = calculateCentroid(file.getChannel(), dimension, totalVectors);
        double radius = calculateRadius(file.getChannel(), dimension, totalVectors, centroid);

        file.seek(RADIUS_POSITION);
        file.writeDouble(radius);
        putVectorToFile(file, centroid);
        log.info("root node, calculated radius: {}", radius);
        return centroid;
    }

    private static RealVector getCentroid(RandomAccessFile file, int dimension) throws IOException {
        file.seek(CENTROID_POSITION);
        return readVectorFromFile(file.getChannel(), dimension);
    }
}
