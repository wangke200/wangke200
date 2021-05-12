package com.tiezh.hash;

import org.apache.commons.codec.binary.Base64;

import java.io.UnsupportedEncodingException;

public class ApacheBase64Util {
    private static Base64 base64;
    private static Base64 base64Safe;
    private static String UTF_8 ="UTF-8";

    static {
        base64 = new Base64();
        base64Safe = new Base64(true);
    }

    //base encode
    public static byte[] encode(byte[] bytes){
        return base64.encode(bytes);
    }

    public static String encode2String(byte[] bytes){
        return base64.encodeToString(bytes);
    }
    public static byte[] encode2Byte(String string){
        try {
            return base64.encode(string.getBytes(UTF_8));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }
    public static String encode(String strings){
        try {
            return base64.encodeToString(strings.getBytes(UTF_8));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }
    //safe Base64 (URL Base64)
    public static byte[] safeEncode(byte[] bytes){
        return base64Safe.encode(bytes);
    }

    public static String safeEncode2String(byte[] bytes){
        return base64Safe.encodeToString(bytes);
    }
    public static byte[] safeEncode(String string){
        try {
            return base64Safe.encode(string.getBytes(UTF_8));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }
    public static String safeEncode2Byte(String strings){
        try {
            return base64Safe.encodeToString(strings.getBytes(UTF_8));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }
    //decode
    public static byte[] decode(byte[] bytes){
        return base64.decode(bytes);
    }

    public static String decode2String(byte[] bytes){
        try {
            return new String(base64.decode(bytes),UTF_8);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }
    public static byte[] decode(String string){
        try {
            return base64.decode(string.getBytes(UTF_8));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }
    public static String decode2Byte(String strings){
        try {
            return new String(decode(strings.getBytes(UTF_8)));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    //safeDecode
    public static byte[] safeDecode(byte[] bytes){
        return base64Safe.decode(bytes);
    }

    public static String safeDecode2String(byte[] bytes){
        try {
            return new String(base64Safe.decode(bytes),UTF_8);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }
    public static byte[] safeDecode(String string){
        try {
            return base64Safe.decode(string.getBytes(UTF_8));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }
    public static String safeDecode2Byte(String strings){
        try {
            return new String(safeDecode(strings.getBytes(UTF_8)));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }
}