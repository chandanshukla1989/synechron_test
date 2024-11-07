package org.example;

import java.time.LocalDate;
import java.time.Period;

public class Person {
    private String name;
    private String address;
    private LocalDate dateOfBirth;

    public Person(String name, String address, LocalDate dateOfBirth) {
        this.name = name;
        this.address = address;
        this.dateOfBirth = dateOfBirth;
    }

    public String getName() {
        return name;
    }

    public String getAddress() {
        return address;
    }

    public LocalDate getDateOfBirth() {
        return dateOfBirth;
    }

    public int getAge() {
        return Period.between(this.dateOfBirth, LocalDate.now()).getYears();
    }

    @Override
    public String toString() {
        return "Person{name='" + name + "', address='" + address + "', dateOfBirth=" + dateOfBirth + '}';
    }
}
