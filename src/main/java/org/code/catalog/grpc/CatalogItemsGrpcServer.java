package org.code.catalog.grpc;


import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;


@GrpcService
@RequiredArgsConstructor
@Slf4j
public class CatalogItemsGrpcServer extends CatalogItemServiceGrpc.CatalogItemServiceImplBase {
    private final CatalogProductWrapperService catalogProductWrapperService;


    @Override
    public void getCatalogItem(CatalogItemRequest request, StreamObserver<CatalogProductResponse> responseObserver) {
        try {
            responseObserver.onNext(catalogProductWrapperService.getCatalogItem(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            var status = handleException(e);
            responseObserver.onError(status.asException());

        }
    }

    @Override
    public void getCatalogByCatalogIdFilter(CatalogIdFilter request, StreamObserver<CatalogProductResponse> responseObserver) {
        try {
            responseObserver.onNext(catalogProductWrapperService.getCatalogByCatalogIdFilter(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            // Attach error details to the metadata
            responseObserver.onError(Status.INTERNAL
                .withDescription("Error fetching catalog item filter: " + e.getMessage())
                .asRuntimeException());
        }
    }

    @Override
    public void getPaginatedCatalogItems(GetPaginatedCatalogItemRequest request, StreamObserver<GetPaginatedCatalogItemResponse> responseObserver) {
        try {
            responseObserver.onNext(catalogProductWrapperService.getPaginatedCatalogItems(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            // Attach error details to the metadata
            responseObserver.onError(Status.INTERNAL
                .withDescription("Error Fetching paginated catalog: " + e.getMessage())
                .asRuntimeException());
        }
    }


    public Status handleException(Exception e) {
        if (e instanceof NotFoundException) {
            return Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e);

        } else if (e instanceof BadRequestException) {
            return Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e);
        } else {
            return Status.INTERNAL.withDescription(e.getMessage()).withCause(e);
        }
    }
}
