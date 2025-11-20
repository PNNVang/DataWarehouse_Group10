package com.example.demo.repository;

import com.example.demo.entity.Mart;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.Date;
import java.util.List;
@Repository
public interface MartRepository extends JpaRepository<Mart, Long> {
    List<Mart> findAll();
    @Query("SELECT m.numberValue\n" +
            "FROM Mart m \n" +
            "WHERE m.totalOccurrences = (\n" +
            "SELECT MAX(m.totalOccurrences)\n" +
            "FROM Mart m\n" +
            ")")
    int mostNumber();
    @Query("SELECT m.numberValue\n" +
            "FROM Mart m \n" +
            "WHERE m.totalOccurrences = (\n" +
            "SELECT MIN(m.totalOccurrences)\n" +
            "FROM Mart m\n" +
            ")")
    int leastNumber();
    @Query("SELECT MAX(m.lastAppearedDate)\n" +
            "FROM Mart m")
    LocalDate lastUpdate();
    @Query("SELECT MAX(m.totalDraws)\n" +
            "FROM Mart m")
    int totalDraws();
}
