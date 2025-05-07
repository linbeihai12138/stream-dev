package com.zsf.realtime.common.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @Package com.stream.utils.SensitiveWordsUtils
 * @Author zhou.han
 * @Date 2025/3/16 21:58
 * @description: sensitive words
 */
public class SensitiveWordsUtils {


    //从指定文件中读取敏感词列表，并将每行内容作为一个敏感词添加到 ArrayList 中。
    public static ArrayList<String> getSensitiveWordsLists(){
        ArrayList<String> res = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader("E:\\777github仓库\\stream-dev\\stream-common\\src\\main\\resources\\Identify-sensitive-words.txt"))){
            String line ;
            while ((line = reader.readLine()) != null){
                res.add(line);
            }
        }catch (IOException ioException){
            ioException.printStackTrace();
        }
        return res;
    }

    //从给定的列表中随机选取一个元素并返回。
    public static <T> T getRandomElement(List<T> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        Random random = new Random();
        int randomIndex = random.nextInt(list.size());
        return list.get(randomIndex);
    }

    public static void main(String[] args) {
        System.err.println(getRandomElement(getSensitiveWordsLists()));
    }
}
