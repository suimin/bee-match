package com.bee.match.exception;

public class CloneFailureException extends MatchException{
    public CloneFailureException() {
    }

    public CloneFailureException(String message) {
        super(message);
    }
}
