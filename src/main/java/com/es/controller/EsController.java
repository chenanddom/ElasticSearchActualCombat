package com.es.controller;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutionException;

@RestController
public class EsController {
    @Autowired
    private TransportClient client;

    @GetMapping("/get/book/_doc")
    public ResponseEntity get(@RequestParam(name = "id", defaultValue = "") String id) {
        if (id.isEmpty()) {
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }
        GetResponse result = this.client.prepareGet("book", "_doc", id).get();

        if (!result.isExists()) {
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity(result.getSource(), HttpStatus.OK);
    }

    /**
     * 添加文档
     * @param title
     * @param author
     * @param wordCount
     * @param publishDate
     * @return
     */
    @PostMapping("/add/book/_doc")
    public ResponseEntity addBook(@RequestParam("title")String title,
                                  @RequestParam("author")String author,
                                  @RequestParam("word_count")Integer wordCount,
                                  @RequestParam("publish_date")
                                  @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")Date publishDate){
        try {
            XContentBuilder content = XContentFactory
                    .jsonBuilder()
                    .startObject()
                    .field("title", title)
                    .field("author", author)
                    .field("word_count", wordCount)
                    .field("publish_date", publishDate.getTime())
                    .endObject();
            IndexResponse indexResponse = this.client.prepareIndex("book", "_doc")
                    .setSource(content).get();
            return new ResponseEntity(indexResponse.getId(),HttpStatus.OK);
        } catch (IOException e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /***
     * 删除文档
     * @param id
     * @return
     */
    @DeleteMapping("/update/book/_doc")
    public ResponseEntity deleteBook(@RequestParam("id")String id){
        if (id.isEmpty()){
            return new ResponseEntity(HttpStatus.BAD_REQUEST);
        }
        DeleteRequestBuilder deleteRequestBuilder = this.client.prepareDelete("book", "_doc", id);
        return new ResponseEntity(deleteRequestBuilder.get().toString(),HttpStatus.OK);
    }

    @PutMapping("/put/book/_doc")
    public ResponseEntity addBook(@RequestParam(value = "id",required = true)String id,
                                  @RequestParam(value = "title",required = false)String title,
                                  @RequestParam(value = "author",required = false)String author,
                                  @RequestParam(value = "word_count",required = false)Integer wordCount,
                                  @RequestParam(value = "publish_date",required = false)
                                  @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")Date publishDate){

        UpdateRequest updateRequest = new UpdateRequest("book","_doc",id);
        try {
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
            if (!StringUtils.isEmpty(title)){
                xContentBuilder.field("title",title);
            }
            if (!StringUtils.isEmpty(author)){
                xContentBuilder.field("author",author);
            }
            if (!StringUtils.isEmpty(wordCount)){
                xContentBuilder.field("word_count",wordCount);
            }

            if (!StringUtils.isEmpty(publishDate)){
                xContentBuilder.field("publish_date",publishDate.getTime());
            }
            xContentBuilder.endObject();
            updateRequest.doc(xContentBuilder);
            UpdateResponse updateResponse = this.client.update(updateRequest).get();
            return new ResponseEntity(updateResponse.getResult(),HttpStatus.OK);
        } catch (IOException e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (ExecutionException e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }


    }






}
