package com.example.demo.dtos;

import java.time.LocalDate;
import java.util.Date;

public class StatisticResponse {
    int totalOccurrences;
    int mostNumber;
    int leastNumber;
    LocalDate lastUpdate;

    public StatisticResponse(int totalOccurrences, int mostNumber, int leastNumber, LocalDate  lastUpdate) {
        this.totalOccurrences = totalOccurrences;
        this.mostNumber = mostNumber;
        this.leastNumber = leastNumber;
        this.lastUpdate = lastUpdate;
    }

    public StatisticResponse() {
    }

    @Override
    public String toString() {
        return "StatisticResponse{" +
                "totalOccurrences=" + totalOccurrences +
                ", mostNumber=" + mostNumber +
                ", leastNumber=" + leastNumber +
                ", lastUpdate=" + lastUpdate +
                '}';
    }

    public int getTotalOccurrences() {
        return totalOccurrences;
    }

    public void setTotalOccurrences(int totalOccurrences) {
        this.totalOccurrences = totalOccurrences;
    }

    public int getMostNumber() {
        return mostNumber;
    }
    public void setMostNumber(int mostNumber) {
        this.mostNumber = mostNumber;
    }

    public int getLeastNumber() {
        return leastNumber;
    }

    public void setLeastNumber(int leastNumber) {
        this.leastNumber = leastNumber;
    }

    public LocalDate  getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(LocalDate  lastUpdate) {
        this.lastUpdate = lastUpdate;
    }
}
