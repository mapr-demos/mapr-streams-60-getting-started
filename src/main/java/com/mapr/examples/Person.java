package com.mapr.examples;
/*
* This is the data class for the POJO Consumer/Producer example
* */

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class Person implements Serializable {
    String id=null;
    int age=0;
    List<String> hobbies=null;
    String address=null;
    List<String> skills = new LinkedList<>();
    List<String> languages = new LinkedList<>();;

    public String getId() { return id; }
    public void setId(String id) { this.id=id; }

    public int getAge() { return age; }
    public void setAge(int age) { this.age = age; }

    public List<String> getHobbies() { return hobbies; }
    public void setHobbies(List<String> hobbies) { this.hobbies = hobbies; }

    public String getAddress() { return address; }
    public void setAddress(String address) { this.address = address; }

    public List<String> getSkills() { return skills; }
    public void setSkills(List<String> skills) { this.skills = skills; }

    public List<String> getLanguages() { return languages; }
    public void setLanguages(List<String> languages) { this.languages = languages; }
}