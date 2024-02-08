package org.code.catalog.repository;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;
import reactor.core.publisher.Sinks;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Service
@Slf4j
public class CatalogIndex {

    private final String SEARCH_FIELD = "search_field";
    private final String INDEX_ID_FIELD = "id";
    private final Sinks.Many<Boolean> sink;


    private StandardAnalyzer analyzer;
    private Directory inMemoryIndex;
    private IndexWriterConfig indexWriterConfig;
    private IndexWriter indexWriter;


    private QueryParser queryParser;
    private DirectoryReader indexReader;
    private IndexSearcher searcher;

    public CatalogIndex() {
        try {
            this.sink = Sinks.many().unicast()
                .onBackpressureBuffer();
            this.initializeIndex();

        } catch (IOException e) {
            throw new RuntimeException("Could not Initialize Index: " + e.getMessage());
        }
    }

    void initializeIndex() throws IOException {
        this.analyzer = new StandardAnalyzer();
        createIndexWriter();
        this.queryParser = new QueryParser(SEARCH_FIELD, analyzer);
        this.initializeReaderBatchProcessor();
    }

    private void createIndexWriter() throws IOException {
        this.inMemoryIndex = new ByteBuffersDirectory();  //@TODO compare tradeoffs against MMapDirectory
        this.indexWriterConfig = new IndexWriterConfig(analyzer);
        this.indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        this.indexWriter = new IndexWriter(inMemoryIndex, this.indexWriterConfig);
    }

    /**
     * Since the index reader creation process is relatively expensive I want reduce the number of times it occurs.
     * I therefore create a buffer of events and I do a 1 second interval or 4000 records before I trigger the inder reader creation
     * NB the index reader creation has to be sequential
     */
    private void initializeReaderBatchProcessor() {
        this.sink.asFlux().bufferTimeout(4000, Duration.ofSeconds(1))
            .subscribe(list -> {
                this.createIndexReader();
            });
    }

    private void createIndexReader()  {

        try {
            StopWatch watch = new StopWatch();
            watch.start();
            this.indexWriter.commit();
            this.indexReader = DirectoryReader.open(this.inMemoryIndex);
            this.searcher = new IndexSearcher(this.indexReader);
            watch.stop();
            log.info("Index reader creation time:{}", watch.getTotalTimeMillis());
        }
        catch (Exception e){
            log.error("Issue creating Index Reader",e.getMessage());
            e.printStackTrace();
        }
    }

    @Async
    public void addToIndex(String searchString, String productBundleId) {
        try {
            Document doc = getDocument(searchString, productBundleId);
            this.indexWriter.updateDocument(new Term(INDEX_ID_FIELD, productBundleId), doc);
            this.sink.tryEmitNext(true); // This emits notifying the index to be commited and the reader recreated
        } catch (IOException e) {
            // @TODO collect metrics on Failed indexing
            log.error("Could not add item to index {} {}", productBundleId, e.getMessage());
            e.printStackTrace();
        }
    }

    public List<String> search(@NonNull String searchTerm, int n) throws ParseException, IOException {
        // Ensure the search term isn't empty
        if (searchTerm.trim().isEmpty()) {
            return Collections.emptyList();
        }

        // Escape special characters in the search term
        String escapedSearchTerm = QueryParser.escape(searchTerm);

        Query q = this.queryParser.parse(escapedSearchTerm);

        TopDocs topDocs = this.searcher.search(q, n);

        StoredFields storedFields = this.indexReader.storedFields();
        List<String> sortedProductBundleIds = Arrays.stream(topDocs.scoreDocs)
            .map(scoreDoc -> {
                try {
                    return storedFields.document(scoreDoc.doc);
                } catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .map(doc -> doc.get(INDEX_ID_FIELD))
            .toList();

        return sortedProductBundleIds;
    }


    private Document getDocument(String searchString, String productBundleId) {
        Document doc = new Document();
        doc.add(new TextField(SEARCH_FIELD, searchString, Field.Store.NO));
        doc.add(new StringField(INDEX_ID_FIELD, productBundleId, Field.Store.YES));
        return doc;
    }

}
