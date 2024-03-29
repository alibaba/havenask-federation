package com.example.demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.lucene.analysis.Analyzer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.util.Objects;

@RestController
public class Controller {
    private final ChatService chatService;

    @Autowired
    public Controller(ChatService chatService) {
        this.chatService = chatService;
    }
    @PostMapping("ask")
    public ResponseEntity<String> askQuestion(@RequestBody String questionJsonStr) throws IOException {
        JSONObject questionJson = JSON.parseObject(questionJsonStr);
        String question = questionJson.getString("question");

        String answer = chatService.getAnswer(question);
        if (Objects.isNull(answer)) {
            answer = "not found or error";
        }
        //String result = answer + "\n\n";
        return ResponseEntity.ok(answer);
    }
}