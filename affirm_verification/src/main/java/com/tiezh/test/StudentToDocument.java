package com.tiezh.test;

import java.io.*;
import java.util.*;

public class StudentToDocument {

    public static List<Document> stu2Doc(List<Student> studentList){
        int size = studentList.size();;
        List<Document> documentList = new ArrayList<>(size);
        for(Student student : studentList){
            int id = student.getId();
            int age = student.getAge();
            int score = student.getScore();

            Set<String> keywords = new HashSet<>();
            keywords.add("age=" + age);
            keywords.add("score=" + score);

            Document document = Document.create(Integer.toString(id), keywords);
            documentList.add(document);
        }
        return documentList;
    }


    public static void docs2Files(String dirPath, List<Document> documentList) throws IOException {
        File dir = new File(dirPath);
        if(dir.exists() && !dir.isDirectory()){
            throw new IOException("error: " + dirPath + "is already exist, and it is not a directory");
        }

        if (!dir.exists())
            dir.mkdirs();

        for (Document doc : documentList) {
            String id = doc.getId();

            File docFile = new File(dirPath + "/" + id + ".txt");
            docFile.createNewFile();

            BufferedWriter out = new BufferedWriter(new FileWriter(docFile));
            Iterator<String> iterator = doc.getKeywords().iterator();
            while(iterator.hasNext()){
                out.write(iterator.next());
                out.write("\n");
            }
            out.close();
        }
    }


    public static void main(String[] args) throws IOException {

        String dirPath = "F:/stuDoc";

        int totalSize = 40000;
        int repeatNum = 1;
        int idOffset = 1;
        int valBase = 60;

        // 设置参数
        // 参数： [totalSize] [repeatNum] [dirPath]
        if (args.length >= 1) {
            totalSize = Integer.parseInt(args[0]);
        }
        if (args.length >= 2) {
            repeatNum = Integer.parseInt(args[1]);
            if (repeatNum < 1 || repeatNum > totalSize) {
                System.out.println("Illegal argument: the 2th arg [repeatNum] should be in [1, " + totalSize + "]");
                return;
            }
        }
        if(args.length >= 3) {
            dirPath = args[2];
        }


        // 测试对象
        List<Student> studentList = Student.generateDatas(totalSize, idOffset, valBase, repeatNum);
        List<Document> documentList = stu2Doc(studentList);
        System.out.println("documentList to files...");
        docs2Files(dirPath + "_" + totalSize + "_" + repeatNum, documentList);
        System.out.println("done");
    }
}
