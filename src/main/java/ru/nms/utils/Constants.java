package ru.nms.utils;

import com.google.common.io.Files;
import org.apache.commons.math3.linear.RealVector;

import java.util.Arrays;
import java.util.Comparator;

public class Constants {
    public static final String BALL_TREE_HEADER = "BALLTREEVECTORS";
    public static final int META_DATA_HEADER_SIZE = Long.BYTES + Integer.BYTES;
    public static final int BALL_TREE_HEADER_SIZE = BALL_TREE_HEADER.length();
    public static final int RADIUS_POSITION = META_DATA_HEADER_SIZE + BALL_TREE_HEADER_SIZE;
    public static final int RADIUS_SIZE = Double.BYTES;
    public static final int CENTROID_POSITION = RADIUS_POSITION + Double.BYTES;
    public static final String TEMP_DIR = Files.createTempDir().getAbsolutePath();
    public static final Comparator<RealVector> PROJECTION_COMPARATOR = Comparator.comparingDouble(v -> Arrays.stream(v.toArray()).sum());

}
