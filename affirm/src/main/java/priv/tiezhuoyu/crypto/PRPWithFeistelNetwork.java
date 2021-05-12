package priv.tiezhuoyu.crypto;

import java.io.UnsupportedEncodingException;
import java.security.InvalidParameterException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.MD5Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.crypto.params.KeyParameter;

/**
 * implement a PRP with three level Feistel Network
 * PRP: [N] -> [N], where N is determined by block size
 * 
 * */

public class PRPWithFeistelNetwork {
	// generate level keys and block size
	public static List<byte[]> genKey(String key) {
		List<byte[]> keys = new ArrayList<>();
		
		// generate level key
		Digest md5 = new MD5Digest();
		keys = new ArrayList<>();
		for(int i = 0; i < 3; i ++) {
			byte[] tmpkey = (key + "k" + i).getBytes(); 
			md5.update(tmpkey, 0, tmpkey.length);
			byte[] lk = new byte[md5.getDigestSize()];
			md5.doFinal(lk, 0);
			keys.add(lk);
		}
		
		return keys;
	}
	
	// generate level keys and block size
	public static List<byte[]> genKey(byte[] bytesKey) {
		List<byte[]> keys = new ArrayList<>();
		
		// generate level key
		Digest md5 = new MD5Digest();
		keys = new ArrayList<>();
		for(int i = 0; i < 3; i ++) {
			byte[] tmpkey = (new String(bytesKey) + "k" + i).getBytes(); 
			md5.update(tmpkey, 0, tmpkey.length);
			byte[] lk = new byte[md5.getDigestSize()];
			md5.doFinal(lk, 0);
			keys.add(lk);
		}
		
		return keys;
	}
	
	public static int prp(List<byte[]> keys, int block, int blockSize) {
		int[] lr = new int[2];
		lr[0] = block % (1 << (blockSize / 2));
		lr[1] = block >> (blockSize / 2);
		
		/**
		 * feistel network
		 * */
		for(int i = 0; i < 3; i++) {
			byte[] roundKey = keys.get(i);
			feistelRound(lr, roundKey, blockSize);
		}
		
		
		int output = lr[0];
		output |= lr[1] << (blockSize / 2);
		
		return output;
	}
	
	public static int prpInv(List<byte[]> keys, int block, int blockSize){
		int[] lr = new int[2];
		
		// swap left and right
		lr[1] = block % (1 << (blockSize / 2));
		lr[0] = block >> (blockSize / 2);
		
		/**
		 * feistel network
		 * */
		
		for(int i = 0; i < 3; i++) {
			byte[] roundKey = keys.get(2 - i);
			feistelRound(lr, roundKey, blockSize);
		}
		
		int output = lr[1];
		output |= lr[0] << (blockSize / 2);
		
		return output;
	}
	
	
	// one round feistel cipher
	private static void feistelRound(int[] lr, byte[] lkey, int blockSize){
		int left = lr[0];
		int right = lr[1];
		
		// Li+1 = Ri
		lr[0] = right;
		
		// Ri+1 = Li xor F(Ri, Ki)
		int frk = bytes2Int(hmac(lkey, int2Bytes(right)));
		frk = frk << (32 - (blockSize / 2));
		frk = frk >>> (32 - (blockSize / 2));
		lr[1] = left ^ frk;
	}
	
	
	// int to byte array
	private static byte[] int2Bytes(int integer) {
		byte[] bytes = new byte[4];
		bytes[3] = (byte) (integer >> 24);
		bytes[2] = (byte) (integer >> 16);
		bytes[1] = (byte) (integer >> 8);
		bytes[0] = (byte) integer;
		return bytes;
	}
	
	
	// byte array to int
	private static int bytes2Int(byte[] bytes) {
		int int1 = bytes[0] & 0xff;
		int int2 = (bytes[1] & 0xff) << 8;
		int int3 = (bytes[2] & 0xff) << 16;
		int int4 = (bytes[3] & 0xff) << 24;

		return int1|int2|int3|int4;
	}
	
	// 128-bit hmac
	private static byte[] hmac(byte[] key, byte[] msg) {
		HMac hmac = new HMac(new MD5Digest());
		byte[] result = new byte[hmac.getMacSize()];
		hmac.init(new KeyParameter(key));
		hmac.reset();
		hmac.update(msg, 0, msg.length);
		hmac.doFinal(result, 0);
		return result;
	}
}
