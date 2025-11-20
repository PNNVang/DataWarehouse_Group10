package com.example.demo.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDate;


@Entity
@Table(name = "mart_two_digit_probability")
public class Mart {
    @Id
    @Column(name = "number_value", nullable = false, length = 10)
    private String numberValue;

    @Column(name = "total_occurrences", precision = 32)
    private BigDecimal totalOccurrences;

    @Column(name = "total_draws")
    private Integer totalDraws;

    @Column(name = "probability", precision = 36, scale = 4)
    private BigDecimal probability;

    @Column(name = "last_appeared_date")
    private LocalDate lastAppearedDate;

    @Column(name = "days_since_last")
    private Integer daysSinceLast;

    public String getNumberValue() {
        return numberValue;
    }

    public void setNumberValue(String numberValue) {
        this.numberValue = numberValue;
    }

    public BigDecimal getTotalOccurrences() {
        return totalOccurrences;
    }

    public void setTotalOccurrences(BigDecimal totalOccurrences) {
        this.totalOccurrences = totalOccurrences;
    }

    public Integer getTotalDraws() {
        return totalDraws;
    }

    public void setTotalDraws(Integer totalDraws) {
        this.totalDraws = totalDraws;
    }

    public BigDecimal getProbability() {
        return probability;
    }

    public void setProbability(BigDecimal probability) {
        this.probability = probability;
    }

    public LocalDate getLastAppearedDate() {
        return lastAppearedDate;
    }

    public void setLastAppearedDate(LocalDate lastAppearedDate) {
        this.lastAppearedDate = lastAppearedDate;
    }
    public Integer getDaysSinceLast() {
        return daysSinceLast;
    }

    public void setDaysSinceLast(Integer daysSinceLast) {
        this.daysSinceLast = daysSinceLast;
    }

    @Override
    public String toString() {
        return "Mart{" +
                "numberValue='" + numberValue + '\'' +
                ", totalOccurrences=" + totalOccurrences +
                ", totalDraws=" + totalDraws +
                ", probability=" + probability +
                ", lastAppearedDate=" + lastAppearedDate +
                ", daysSinceLast=" + daysSinceLast +
                '}';
    }
}