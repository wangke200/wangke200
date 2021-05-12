package pku;

import java.math.BigInteger;
import java.util.*;

/**
 * @author Lei, HUANG (lhuang@pku.edu.cn): 2019-04-17 19:37:38
 */
public class Accumulator {

    private static int RSA_KEY_SIZE           = 1024;
    private static int RSA_PRIME_SIZE         = RSA_KEY_SIZE / 2;
    private static int ACCUMULATED_PRIME_SIZE = 128;

    private       BigInteger                  N;
    private       BigInteger                  A;
    private       BigInteger                  A0;
    final private EnhancedRandom              random;
    final private Map<String, BigInteger> data;

    public Accumulator() {
        data = new HashMap<>();
        Pair<BigInteger> bigIntegerPair = Util.generateTwoLargeDistinctPrimes(RSA_PRIME_SIZE);
        BigInteger       p              = bigIntegerPair.getFirst();
        BigInteger       q              = bigIntegerPair.getSecond();
        N = p.multiply(q);
        random = new EnhancedRandom();
        A0 = random.nextBigInteger(BigInteger.ZERO, N);
        A = A0;
    }

    /**
     * get size of all accumulated elements
     * @return size of all accumulated elements
     */
    public int size() {
        return data.size();
    }


    public BigInteger getA(){
        return A;
    }


    public BigInteger getA0(){
        return A0;
    }


    public BigInteger getN() {
        return N;
    }

    public BigInteger getNonce(String x) {
        return data.get(x);
    }

    public List<BigInteger> getNonce(List<String> xs){
        if(xs == null)
            return null;
        List<BigInteger> nonces = new ArrayList<>(xs.size());
        for(String x : xs){
            BigInteger nonce = getNonce(x);
            if (nonce == null)
                return null;
            nonces.add(nonce);
        }
        return nonces;
    }


    public BigInteger add(String x) {
        if (data.containsKey(x)) {
            return A;
        } else {
            BigInteger xBigInteger = str2BigInteger(x);
            Pair<BigInteger> bigIntegerPair = Util.hashToPrime(xBigInteger, ACCUMULATED_PRIME_SIZE);
            BigInteger       hashPrime      = bigIntegerPair.getFirst();
            BigInteger       nonce          = bigIntegerPair.getSecond();
            A = A.modPow(hashPrime, N);
            data.put(x, nonce);
            return A;
        }
    }

    public BigInteger proveMembership(String x) {
        if (!data.containsKey(x)) {
            return null;
        } else {
            BigInteger product = iterateAndGetProduct(x);
            return A0.modPow(product, N);
        }
    }


    public BigInteger proveMembership(List<String> xs) {
        Set<String> xSet = new HashSet<>(xs);
        BigInteger product = iterateAndGetProduct(xSet);
        return A0.modPow(product, N);
    }


    /**
     * Iterate data map
     *
     * @param x
     * @return
     * @author Lei, HUANG (lhuang@pku.edu.cn): 2019-04-17 22:40:13
     */
    private BigInteger iterateAndGetProduct(String x) {
        BigInteger product = BigInteger.ONE;
        for (String k : data.keySet()) {
            //calculate the product of nonce of elements except x itself
            if (k.compareTo(x) != 0) {
                BigInteger nonce = data.get(k);
                BigInteger kBigInteger = new BigInteger(k.getBytes());
                product = product.multiply(
                        // only hashed value needed here
                        Util.hashToPrime(kBigInteger, ACCUMULATED_PRIME_SIZE, nonce).getFirst());
            }
        }
        return product;
    }


    /**
     * Iterate data map
     *
     * @param xSet
     * @return
     * @author Zhuoyu Tie: 2020-11-04
     */
    private BigInteger iterateAndGetProduct(Set<String> xSet) {
        BigInteger product = BigInteger.ONE;
        for (String k : data.keySet()) {
            //calculate the product of nonce of elements except x itself
            if (!xSet.contains(k)) {
                BigInteger nonce = data.get(k);
                BigInteger kBigInteger = new BigInteger(k.getBytes());
                product = product.multiply(
                        // only hashed value needed here
                        Util.hashToPrime(kBigInteger, ACCUMULATED_PRIME_SIZE, nonce).getFirst());
            }
        }
        return product;
    }


    /**
     * delete an element from accumulator and return the updated value
     *
     * @param x ele to delete
     * @return updated accumulator value
     * @author Lei, HUANG (lhuang@pku.edu.cn): 2019-04-17 22:44:51
     */
    public BigInteger delete(String x) {
        if (!data.containsKey(x)) {
            return A;
        } else {
            data.remove(x);
            final BigInteger product = iterateAndGetProduct(x);
            this.A = A0.modPow(product, N);
            return A;
        }
    }

    /**
     * use simple modpow calculation to verify element membership
     *
     * @param A
     * @param x
     * @param proof
     * @param n
     * @return
     * @author Lei, HUANG (lhuang@pku.edu.cn): 2019-04-17 22:45:02
     */
    private static boolean doVerifyMembership(BigInteger A, BigInteger x, BigInteger proof, BigInteger n) {
        return proof.modPow(x, n).compareTo(A) == 0;
    }


    /**
     * use simple modpow calculation to verify element membership
     *
     * @param A
     * @param xs
     * @param nonces
     * @param proof
     * @param n
     * @return
     * @author Zhuoyu Tie: 2020-11-4
     */
    private static boolean doVerifyMembership(BigInteger A, List<String> xs, List<BigInteger> nonces,
                                              BigInteger proof, BigInteger n){
        if(xs.size() != nonces.size())
            throw new IllegalArgumentException("xs size " + xs.size() + "is not equal to nonces size " + nonces.size());
        for(int i = 0; i < xs.size(); i++){
            String x = xs.get(i);
            BigInteger nonce = nonces.get(i);
            BigInteger xBigInteger = new BigInteger(x.getBytes());
            BigInteger xPrime = Util.hashToPrime(xBigInteger, ACCUMULATED_PRIME_SIZE, nonce).getFirst();
            proof = proof.modPow(xPrime, n);
        }
        return proof.compareTo(A) == 0;
    }



    /**
     * verify element membership
     *
     * @param A
     * @param x
     * @param nonce
     * @param proof
     * @param n
     * @return true if element x is accumulated by accumulator A, with proof,nonce and
     */
    public static boolean verifyMembership(
            BigInteger A,
            String x,
            BigInteger nonce,
            BigInteger proof,
            BigInteger n
                                          ) {
        BigInteger xBigInteger = new BigInteger(x.getBytes());
        return doVerifyMembership(A, Util.hashToPrime(xBigInteger, ACCUMULATED_PRIME_SIZE, nonce).getFirst(), proof, n);
    }

    /**
     * verify element membership
     *
     * @param A
     * @param xs
     * @param nonces
     * @param proof
     * @param n
     * @return true if element x is accumulated by accumulator A, with proof,nonce and
     */
    public static boolean verifyMembership(
            BigInteger A,
            List<String> xs,
            List<BigInteger> nonces,
            BigInteger proof,
            BigInteger n
    ) {
        return doVerifyMembership(A, xs, nonces, proof, n);
    }

    /**
     * verify element membership
     * @author Zhuoyu Tie: 2020-11-04
     * @param str
     * @return BigInteger with bytes of string str
     */
    public static BigInteger str2BigInteger(String str){
        return new BigInteger(str.getBytes());
    }
}
