package com.example.demo.controller;

import com.example.demo.entity.Mart;
import com.example.demo.service.MartService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/mart")
public class MartController {
    @Autowired
    private MartService martService;

    @GetMapping("/all")
    public ResponseEntity<?> getAll(){
        System.out.println("có request");
        List<Mart> list = martService.getMarts();
        if(list.isEmpty()){
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(list);
    }
    @GetMapping("/statistic")
    public ResponseEntity<?> getStatistic(){
        return ResponseEntity.ok(martService.getStatistic());
    }
}
