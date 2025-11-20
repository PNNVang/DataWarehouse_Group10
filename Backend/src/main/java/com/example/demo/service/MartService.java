package com.example.demo.service;

import com.example.demo.dtos.StatisticResponse;
import com.example.demo.entity.Mart;
import com.example.demo.repository.MartRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.Date;
import java.util.List;

@Service
public class MartService {
    @Autowired
    private MartRepository martRepository;
    public List<Mart> getMarts() {
        return martRepository.findAll();
    }
    public StatisticResponse getStatistic() {
        StatisticResponse statisticResponse = new StatisticResponse();
        int mostNumber = martRepository.mostNumber();
        int leastNumber = martRepository.leastNumber();
        int totalDraws = martRepository.totalDraws();
        LocalDate date = martRepository.lastUpdate();
        statisticResponse.setMostNumber(mostNumber);
        statisticResponse.setLeastNumber(leastNumber);
        statisticResponse.setLastUpdate(date);
        statisticResponse.setTotalOccurrences(totalDraws);
        return statisticResponse;
    }

    public static void main(String[] args) {
        MartService martService = new MartService();
        List<Mart> marts = martService.getMarts();
        System.out.println(marts);
    }
}
