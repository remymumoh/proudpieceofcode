package org.code.catalog.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.HTTP_VERSION_NOT_SUPPORTED, reason = "Invalid Catalog Event or invalid Object Id")
public class CatalogEventException extends Exception {

    public CatalogEventException(CatalogEvent catalogEvent,String msg) {
        super("Exception from " + catalogEvent.toString() + ":-"+msg);
    }

}
