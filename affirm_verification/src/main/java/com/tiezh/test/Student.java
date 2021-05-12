package com.tiezh.test;

import java.util.*;

public class Student{

    public static final int AGE_BASE= 11;
    public static final int AGE_TOP= 100;

    int id;
    int score;
    int age;

    public Student(int id, int score){
        //主索引属性
        this.id = id;
        //次级索引属性
        this.score = score;
        //普通属性
        this.age = 100 - (id % 90);
    }

    public static class Index {
        Map<Integer, Student> ind;       //index
        Map<Integer, Set<Student>> inv;       //inverted index

        private Index(Map<Integer, Student> ind, Map<Integer, Set<Student>> inv) {
            this.ind = ind;
            this.inv = inv;
        }

        /** get through Ind, etc. student's id */
        public Student getThroughInd(int id){
            return ind.get(id);
        }

        /** get through score, etc. student's score */
        public Set<Student> getThroughInv(int score){
            return inv.get(score);
        }


        /** get id set */
        public Set<Integer> idSet() {
            return ind.keySet();
        }

        /** get score set */
        public Set<Integer> scoreSet() {
            return inv.keySet();
        }

        public static Index create(List<Student> students){
            if(students == null)
                return null;
            Map<Integer, Student> ind = new HashMap<>();
            Map<Integer, Set<Student>> inv = new HashMap<>();

            for(Student stu : students){
                int id = stu.id;
                int score = stu.score;
                int age = stu.age;

                // update ind
                ind.put(id, stu);

                // update inv
                Set<Student> stuSet = null;
                if(inv.containsKey(stu.score)){
                    stuSet = inv.get(stu.score);
                }else{
                    stuSet = new HashSet<>();
                    inv.put(stu.score, stuSet);
                }
                stuSet.add(stu);
            }
            Index index = new Index(ind, inv);
            return index;
        }
    }

    public int getId() {
        return id;
    }

    public int getScore() {
        return score;
    }

    public int getAge() {
        return age;
    }

    public static List<Student> generateDatas(int totalSize, int idOffset, int valBase, int repeatNum){

        LinkedList<Student> students = new LinkedList<>();
        for(int i = idOffset; i < totalSize; i++){
            int id = i;
            Student student = new Student(id, valBase + (i % (totalSize / repeatNum)));
            students.add(student);
        }
        return students;
    }

    public static List<String> getRVList(Collection<Student> students){
        List<String> rvList = new LinkedList<>();
        for(Student stu : students){
            rvList.add(stu.id + "||age" + "||" + stu.age);
        }
        return rvList;
    }

    public static List<String> getIdList(Collection<Student> students){
        List<String> idList = new LinkedList<>();
        for(Student stu : students){
            idList.add(Integer.toString(stu.id));
        }
        return idList;
    }

    public static List<String> getScoreList(Collection<Student> students){
        List<String> scoreList = new LinkedList<>();
        for(Student stu : students){
            scoreList.add(Integer.toString(stu.score));
        }
        return scoreList;
    }

    public static LinkedList<String> getAgeList(Collection<Student> students){
        LinkedList<String> ageList = new LinkedList<>();
        for(Student stu : students){
            ageList.add(Integer.toString(stu.age));
        }
        return ageList;
    }


    public static Index createIndex(List<Student> students){
        return Index.create(students);
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
                "id=" + id +
                ", score=" + score +
                ", age=" + age +
                '}';
    }
}
