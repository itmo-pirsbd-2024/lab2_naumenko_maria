package ru.nms;

import com.google.common.collect.MinMaxPriorityQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.linear.RealVector;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static ru.nms.utils.BallTreeUtils.*;
import static ru.nms.utils.Constants.BALL_TREE_HEADER_SIZE;
import static ru.nms.utils.Constants.TEMP_DIR;

@Slf4j
public class KnnService {
    public static List<RealVector> knn(RealVector target, int k, String rootFilePath, int nodeSize) throws IOException {
        Comparator<RealVector> comparator = Comparator.comparingDouble(v -> v.getDistance(target));
        MinMaxPriorityQueue<RealVector> knnQueue = null;
        try (RandomAccessFile file = new RandomAccessFile(rootFilePath, "r");
             FileChannel fileChannel = file.getChannel()) {
            verifyFileFormat(fileChannel, rootFilePath);

            fileChannel.position(BALL_TREE_HEADER_SIZE);
            long totalVectors = file.readLong();
            int dimension = file.readInt();

            int firstVectorsAmount = (int) Math.min(k, totalVectors);
            List<RealVector> firstVectors = new ArrayList<>(firstVectorsAmount);
            setPositionToStartOfVectorData(fileChannel, dimension);

            for (int i = 0; i < firstVectorsAmount; i++) {
                firstVectors.add(readVectorFromFile(fileChannel, dimension));
            }

            if (totalVectors <= k) {
                //log.warn("total vectors: {}, k: {} - exiting", totalVectors, k);
                return readVectorsFromFile(fileChannel, dimension, (int) totalVectors);
            }

            knnQueue = MinMaxPriorityQueue
                    .orderedBy(comparator)
                    .maximumSize(k)
                    .create(firstVectors);

        }
        search(knnQueue, target, rootFilePath, nodeSize);
        return knnQueue.stream().sorted(comparator).toList();
    }

    private static void search(MinMaxPriorityQueue<RealVector> queue, RealVector target, String filePath, int nodeSize) throws IOException {
        long leftChildName;
        long rightChildName;
        try (RandomAccessFile file = new RandomAccessFile(filePath, "r");
             FileChannel fileChannel = file.getChannel()) {

            fileChannel.position(BALL_TREE_HEADER_SIZE);
            long totalVectors = file.readLong();
            int dimension = file.readInt();
            double radius = file.readDouble();

            RealVector centroid = readVectorFromFile(fileChannel, dimension);
            leftChildName = file.readLong();
            rightChildName = file.readLong();

            //log.info("searching in the node {} with totalVectors: {}, dimension: {}, radius: {}, left child: {}, right child: {}",
//                    filePath, totalVectors, dimension, radius, leftChildName, rightChildName);
            if (centroid.getDistance(target) - radius >= target.getDistance(queue.peekLast())) {
                return;
            } else if (leftChildName == -1) {
                readVectorsFromFile(fileChannel, dimension, nodeSize)
                        .forEach(vector -> {
                            if (vector.getDistance(target) < target.getDistance(queue.peekLast()) && !queue.contains(vector)) {
                                queue.add(vector);
                            }
                        });
                return;
            }
        }

        search(queue, target, TEMP_DIR + leftChildName, nodeSize);
        search(queue, target, TEMP_DIR + rightChildName, nodeSize);
    }
}
