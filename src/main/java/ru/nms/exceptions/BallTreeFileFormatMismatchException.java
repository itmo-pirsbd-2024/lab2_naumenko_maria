package ru.nms.exceptions;

import static ru.nms.utils.Constants.BALL_TREE_HEADER;

public class BallTreeFileFormatMismatchException extends RuntimeException{

    private static final String ERROR_MESSAGE_TEMPLATE = "File %s is not in ball tree format. Actual file header: %s, wanted header: %s";
    public BallTreeFileFormatMismatchException(String filePath, String header) {
        super(ERROR_MESSAGE_TEMPLATE.formatted(filePath, header, BALL_TREE_HEADER));
    }
}
