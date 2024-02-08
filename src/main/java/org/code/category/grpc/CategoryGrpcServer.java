package org.code.category.grpc;


import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
@AllArgsConstructor
@Slf4j
public class CategoryGrpcServer extends CategoryServiceGrpc.CategoryServiceImplBase {
    final CategoryService categoryService;

    @Override
    public void fetchItemCategories(GetItemCategoriesRequest request, StreamObserver<GetItemCategoriesResponse> responseObserver) {
       try {
           var categoriesResponse = categoryService.fetchCategories(request.getMarketId());
           responseObserver.onNext(categoriesResponse);
           responseObserver.onCompleted();
       }catch (Exception e){
           // Attach error details to the metadata
           responseObserver.onError(Status.INTERNAL
               .withDescription("Error fetching Item Categories: " + e.getMessage())
               .asRuntimeException());
       }
    }
}
