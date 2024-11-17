package ru.nms;

import org.apache.commons.math3.linear.RealVector;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static ru.nms.BruteForceKnnService.bruteForceKnn;
import static ru.nms.TestUtils.*;
import static ru.nms.utils.Constants.TEMP_DIR;

class BallTreeTest {

    private static final String TEST_FILE_NAME = "test.tmp";
    private static final int K = 100;

    @Test
    void shouldFindKNearestNeighbours() throws IOException {
        //given
        List<RealVector> vectors = TestDataGenerator.generateNVectors(RANDOM, 10000, 256);
        writeVectorsToFile(vectors, TEMP_DIR + TEST_FILE_NAME);
        RealVector target = vectors.getLast();
        Comparator<RealVector> comparator = Comparator.comparingDouble(v -> v.getDistance(target));
        List<RealVector> actualNeighbours = bruteForceKnn(new ArrayList<>(vectors), K, target);

        //when
        BallTreeBuilder.build(TEMP_DIR + TEST_FILE_NAME, true, 100);
        List<RealVector> neighbours = KnnService.knn(target, K, TEMP_DIR + TEST_FILE_NAME).stream().sorted(comparator).toList();

        //then
        assertIterableEquals(actualNeighbours, neighbours);
        cleanTempDir();
    }
}
