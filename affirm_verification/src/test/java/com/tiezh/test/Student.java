package com.tiezh.test;

import java.util.LinkedList;
import java.util.Objects;

public class Student{
    int id;
    int score;

    public Student(int id, int score){
        this.id = id;
        this.score = score;
    }

    public static LinkedList<Student> generateDatas(int totalSize, int idOffset, int valBase, int repeatNum){

        LinkedList<Student> students = new LinkedList<>();
        for(int i = idOffset; i < totalSize; i++){
            int id = i;
            Student student = new Student(id, valBase + (i % (totalSize / repeatNum)));
            students.add(student);
        }
        return students;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Student student = (Student) o;
        return score == student.score && id == student.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, score);
    }

    @Override
    public String toString() {
        return "Student{" +
                "id='" + id + '\'' +
                ", score=" + score +
                '}';
    }
}
