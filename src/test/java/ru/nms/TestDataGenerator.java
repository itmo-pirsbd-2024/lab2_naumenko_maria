package ru.nms;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestDataGenerator {

    public static List<RealVector> generateNVectors(Random random, int n, int dimension) {
        List<RealVector> vectors = new ArrayList<>(n);

        for (int i = 0; i < n; i++) {
            double[] vectorData = new double[dimension];
            for (int j = 0; j < dimension; j++) {
                vectorData[j] = random.nextDouble();
            }
            vectors.add(new ArrayRealVector(vectorData));
        }
        return vectors;
    }
}
