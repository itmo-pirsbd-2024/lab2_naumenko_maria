package ru.nms;

import org.apache.commons.math3.linear.RealVector;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static ru.nms.BruteForceKnnService.bruteForceKnn;
import static ru.nms.utils.BallTreeUtils.putVectorToFile;
import static ru.nms.utils.BallTreeUtils.setPositionToStartOfVectorData;
import static ru.nms.utils.Constants.BALL_TREE_HEADER;
import static ru.nms.utils.Constants.ROOT_DIR;

class BallTreeTest {

    private static final Random RANDOM = new Random(0);
    private static final String TEST_FILE_NAME = "test.tmp";
    private static final int K = 100;

    @Test
    void shouldFindKNearestNeighbours() throws IOException {
        //given
        List<RealVector> vectors = TestDataGenerator.generateNVectors(RANDOM, 1000, 256);
        writeVectorsToFile(vectors);
        RealVector target = vectors.getLast();
        Comparator<RealVector> comparator = Comparator.comparingDouble(v -> v.getDistance(target));
        List<RealVector> actualNeighbours = bruteForceKnn(new ArrayList<>(vectors), K, target);

        //when
        BallTreeBuilder.build(TEST_FILE_NAME, true);
        List<RealVector> neighbours = KnnService.knn(target, K, TEST_FILE_NAME).stream().sorted(comparator).toList();

        //then
        assertIterableEquals(actualNeighbours, neighbours);
    }

    private void writeVectorsToFile(List<RealVector> vectors) throws IOException {
        if (vectors.isEmpty()) {
            throw new IllegalArgumentException("The list of vectors is empty.");
        }

        int dimension = vectors.get(0).getDimension();
        long vectorCount = vectors.size();

        try (RandomAccessFile file = new RandomAccessFile(ROOT_DIR + TEST_FILE_NAME, "rw");
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

}
