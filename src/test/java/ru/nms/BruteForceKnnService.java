package ru.nms;

import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.commons.math3.linear.RealVector;

import java.util.Comparator;
import java.util.List;

public class BruteForceKnnService {

    public static List<RealVector> bruteForceKnn(List<RealVector> vectors, int k, RealVector target) {
        Comparator<RealVector> comparator = Comparator.comparingDouble(v -> v.getDistance(target));
        var queue = MinMaxPriorityQueue.orderedBy(comparator)
                .maximumSize(k)
                .create(vectors);
        return queue.stream().sorted(comparator).toList();
    }
}
