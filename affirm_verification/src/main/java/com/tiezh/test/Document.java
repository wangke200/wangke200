package com.tiezh.test;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.*;

public class Document {
    String id;
    Set<String> keywords;

    private static final String[] FILE_HEADER = {"ID","Keywords"};

    public static class Index{
        Map<String, Set<String>> ind;       //index
        Map<String, Set<String>> inv;       //inverted index

        private Index(Map<String, Set<String>> ind, Map<String, Set<String>> inv){
            this.ind = ind;
            this.inv = inv;
        }

        /** get id set */
        public Set<String> idSet() {
            return ind.keySet();
        }

        /** get keyword set */
        public Set<String> keywordSet(){
            return inv.keySet();
        }

        /** get ids containing keyword */
        public Set<String> getIds(String keyword){
            return inv.get(keyword);
        }

        /** get keywords belong to id */
        public Set<String> getKeywords(String id){
            return ind.get(id);
        }


        /** build a Index obj according to documents */
        public static Index create(List<Document> documents){
            if(documents == null)
                return null;

            Map<String, Set<String>> ind = new HashMap<>();
            Map<String, Set<String>> inv = new HashMap<>();

            for(Document doc : documents){
                String id = doc.getId();
                Set<String> keywords = doc.getKeywords();

                // update ind
                if(ind.containsKey(id)){
                    Set<String> kws = new HashSet<>(keywords);
                    ind.put(id, kws);
                }

                // update inv
                for(String kw : keywords){
                    Set<String> ids = null;
                    if(!inv.containsKey(kw)) {
                        ids = new HashSet<>();
                        inv.put(kw, ids);
                    }
                    else
                        ids = inv.get(kw);
                    ids.add(id);
                }
            }

            Index index = new Index(ind, inv);
            return index;
        }
    }


    private Document(String id, Set<String> keywords){
        if(id == null || keywords == null)
            throw new NullPointerException("document's id or keywords are null");
        this.id = id;
        this.keywords = keywords;
    }

    public String getId() {
        return id;
    }

    public Set<String> getKeywords() {
        return keywords;
    }


    /** return a Document obj according to a id and keywords */
    public static Document create(String id, Set<String> keywords){
        if(id == null)
            throw new NullPointerException("id can not be null");
        Document doc = new Document(id, keywords);
        return doc;
    }


    /** return a Document obj according to a file */
    public static Document create(File file) throws IOException {
        if(!file.exists())
            throw new FileNotFoundException("can not find file");

        // get content of file
        FileInputStream is = new FileInputStream(file);
        InputStreamReader ir = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(ir);
        StringBuilder strBuilder = new StringBuilder();
        String line = null;
        while((line = br.readLine()) != null)
            strBuilder.append(line).append("\n");
        br.close();
        ir.close();
        is.close();

        //regular expression get keywords
        String[] keywordArray = strBuilder.toString().split("[^a-zA-Z]+");
        HashSet<String> keywords = new HashSet<>();
        for(String kw : keywordArray){
            if(kw.length() > 0)
                keywords.add(kw);
        }

        //Document object
        Document doc = new Document(file.getAbsolutePath(), keywords);

        return doc;
    }

    /** create CSV file according to documents */
    public static void createCSV(List<Document> documents, String path){
        final String FILE_NAME = path;

        // skip header
        CSVFormat format = CSVFormat.DEFAULT.withHeader(FILE_HEADER);
        try(Writer out = new FileWriter(FILE_NAME);
            CSVPrinter printer = new CSVPrinter(out, format)) {
            for (Document doc : documents) {
                List<String> records = new ArrayList<>();
                records.add(doc.getId());
                records.add(doc.getKeywords().toString());
                printer.printRecord(records);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /** create index for documents */
    public static Index createIndex(List<Document> documents){
        return Index.create(documents);
    }


    /** return all of documents under a directory path */
    public static List<Document> listFromDir(String path) throws IOException {
        File file = new File(path);
        if(!file.exists())
            throw new FileNotFoundException("can not find file or directory: " + path);
        List<File> files = findAllFile(file);
        List<Document> documents = new ArrayList<>();
        for(File f : files){
            documents.add(create(f));
        }
        return documents;
    }


    public static List<Document> listFromCSV(String path) throws IOException {
        File file = new File(path);
        if(!file.exists())
            throw new FileNotFoundException("can not find file or directory: " + path);

        String FILE_NAME = path;

        // an empty documents list
        List<Document> documents = new ArrayList<>();

        //skip header
        CSVFormat format = CSVFormat.DEFAULT.withHeader(FILE_HEADER).withSkipHeaderRecord();

        try(Reader in = new FileReader(FILE_NAME)) {
            Iterable<CSVRecord> records = format.parse(in);
            String id = null;
            Set<String> keywords = null;
            for (CSVRecord record : records) {
                id = record.get(FILE_HEADER[0]);
                String[] kwArray = record.get(FILE_HEADER[1]).split("[^a-zA-Z]+");
                keywords = new HashSet<String>();
                for(String kw : kwArray){
                    if(kw.length() > 0)
                        keywords.add(kw);
                }
                documents.add(new Document(id, keywords));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return documents;
    }

    /** return all of file under a path file */
    private static List<File> findAllFile(File file){
        if(file == null || !file.exists())
            return null;
        List<File> files = new ArrayList<>();
        File dir = file;
        if(!file.isDirectory())
            dir = file.getParentFile();
        findAllFile(dir, files);
        return files;
    }

    /** find all of file under a path file, and fill them into a list */
    private static void findAllFile(File dir, List<File> fileList) {
        if (!dir.exists() || !dir.isDirectory()) {// 判断是否存在目录
            return;
        }
        File[] files = dir.listFiles();// 读取目录下的所有目录文件信息
        for (int i = 0; i < files.length; i++) {// 循环，添加文件名或回调自身
            File file = files[i];
            if (file.isFile()) {// 如果文件
                fileList.add(file);// 添加文件全路径名
            } else {// 如果是目录
                findAllFile(file, fileList);// 回调自身继续查询
            }
        }
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Document document = (Document) o;
        return Objects.equals(id, document.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "Document{" +
                "id='" + id + '\'' +
                ", keywords=" + keywords +
                '}';
    }



    public static void main(String[] args) throws IOException {
        String[] paths = {"F:\\eclipse-workspace\\VDERS\\doc_generate_4000\\"
                        , "F:\\eclipse-workspace\\VDERS\\doc_generate_8000\\"
                        , "F:\\eclipse-workspace\\VDERS\\doc_generate_12000\\"
                        , "F:\\eclipse-workspace\\VDERS\\doc_generate_16000\\"
                        , "F:\\eclipse-workspace\\VDERS\\doc_generate_20000\\"};

        String[] csvPaths = {".\\doc_4000.csv"
                            ,".\\doc_8000.csv"
                            ,".\\doc_12000.csv"
                            ,".\\doc_16000.csv"
                            ,".\\doc_20000.csv"};


//        // turn into csv file
//        for(int i = 0; i < paths.length; i++){
//            System.out.println("generate documents for " + paths[i] + "...");
//            List<Document> documents = listFromDir(paths[i]);
//            System.out.println("documents done");
//
//            System.out.println("generate documents...");
//            Index index = createIndex(documents);
//            System.out.println("index done");
//
//            System.out.println("generate csv file...");
//            createCSV(documents, csvPaths[i]);
//            System.out.println("csv file done");
//        }

        System.out.println("generate documents for " + csvPaths[4] + "...");
        List<Document> documents = listFromCSV(csvPaths[4]);
        System.out.println("documents done");

        System.out.println("generate index ...");
        Index index = createIndex(documents);
        System.out.println("index done");

        System.out.println(index.keywordSet().size());
    }
}
