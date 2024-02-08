package org.code.utils;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
@Builder
public class ResponseWrapper<T> {
    private final int code;
    private final String message;
    private T data;
}
