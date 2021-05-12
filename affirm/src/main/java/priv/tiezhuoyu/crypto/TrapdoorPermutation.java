package priv.tiezhuoyu.crypto;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;


public class TrapdoorPermutation {
	public static KeyPair genKeyPair(int keysize) throws NoSuchAlgorithmException {
		KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("RSA"); 
		keyPairGen.initialize(keysize);
		KeyPair keyPair = keyPairGen.generateKeyPair();
		return keyPair;
	}
	
	public static BigInteger tP(RSAPublicKey pk, BigInteger input) {
		BigInteger N = pk.getModulus();
		BigInteger e = pk.getPublicExponent();
		return input.modPow(e, N);
	}
	
	public static BigInteger tP(RSAPrivateKey sk, BigInteger input) {
		BigInteger N = sk.getModulus();
		BigInteger d = sk.getPrivateExponent();
		return input.modPow(d, N);
	}
}
