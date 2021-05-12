package com.tiezh.affirm;

import com.google.common.base.Charsets;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.primitives.Bytes;
import com.tiezh.hash.ApacheBase64Util;
import com.tiezh.hash.MultiSetHash;
import com.tiezh.hash.MultiSetHashStrategies;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.*;

public class AFFIRMVHT {
    static final int RSA_KEY_SIZE = 1024;
    static final int COUNTER_SIZE = 128;

    //secret key
    //pk and its trapdoor permutation is not for the time being
    byte[] ke, kh, pk;

    //hash strategy, implement g3
    MultiSetHashStrategies.HASH_FUNCTION_WITHKEY strategyG3;
    //hash strategy, implement h4
    MultiSetHashStrategies.HASH_FUNCTION strategyH4;
    //hash strategy, implement multi set hash
    MultiSetHashStrategies.HASH_FUNCTION_WITHKEY strategyMH;

    //funnel, called by hash function in guava
    private static final Funnel strFunnel = Funnels.stringFunnel(Charsets.UTF_8);
    private static final Funnel byteArrayFunnel = Funnels.byteArrayFunnel();

    //to generate random update counter
    SecureRandom secureRandom;

    //verifiable hash table, <alpha, MH>
    private Map<String, String> vht;
    //update counter table, <cv||v, u>
    private Map<String, BigInteger> uct;
    //order list of v on cv, integer from small to big
    private Map<String, List<Integer>> cvt;


    public static class GetToken{
        byte[] t3;
        byte[] u;

        private GetToken(byte[] t3, byte[] u){
            this.t3 = t3;
            this.u = u;
        }

        public static GetToken create(byte[] t3, byte[] u){
            return new GetToken(t3, u);
        }

        public byte[] getT3() {
            return t3;
        }

        public byte[] getU() {
            return u;
        }
    }


    private AFFIRMVHT(byte[] ke, byte[] kh, Map<String, String> vht,
                      MultiSetHashStrategies.HASH_FUNCTION_WITHKEY strategyG3,
                      MultiSetHashStrategies.HASH_FUNCTION strategyH4,
                      MultiSetHashStrategies.HASH_FUNCTION_WITHKEY strategyMH){
        byte[] kee = strategyG3.getKey();
        // check if ke equals to G3 key
        if(ke.length != kee.length)
            throw new IllegalArgumentException("ke length is different from strategyG3.key.length");
        for(int i = 0; i < ke.length; i++){
            if(ke[i] != kee[i])
                throw new IllegalArgumentException("ke length is different from strategyG3.key.length");
        }
        // check if kh equals to MH key
        if(ke.length != kee.length)
            throw new IllegalArgumentException("ke length is different from strategyG3.key.length");
        for(int i = 0; i < ke.length; i++){
            if(ke[i] != kee[i])
                throw new IllegalArgumentException("ke length is different from strategyG3.key.length");
        }
        this.ke = ke;
        this.vht = vht;
        this.strategyG3 = strategyG3;
        this.strategyH4 = strategyH4;
        this.strategyMH = strategyMH;
        secureRandom = new SecureRandom();
        uct = new HashMap<>();
        cvt = new HashMap<>();
    }


    public Map getVHT(){return vht;}



    /** build
     * @param rs record ids
     * @param cvName attribute name
     * @param cvVals values of cvName
     * @param crName attribute name
     * @param crVals values of crName
     * */
    public void build(List<String> rs, String cvName, List<String> cvVals, String crName, List<String> crVals){
        HashMap<String, MultiSetHash> mht = new HashMap<> ();
        if(cvVals.size() != crVals.size())
            throw new IllegalArgumentException("cvVals size " + cvVals.size() + "is not equal to crVals size " + crVals.size());

        int len = cvVals.size();

        //put <cv||v, mh(rv)> into table vmh
        for(int i = 0; i < len; i++){
            String r = rs.get(i);
            String cvVal = cvVals.get(i);
            String crVal = crVals.get(i);
            String rv = r + "||" + crName + "||" + crVal;

            // multiset hash and add
            MultiSetHash<String> multiSetHash = null;
            if(mht.containsKey(cvVal))
                multiSetHash = mht.get(cvVal);
            else{
                multiSetHash = MultiSetHash.create(strFunnel, strategyMH);
                mht.put(cvVal, multiSetHash);
            }
            multiSetHash.add(rv);
        }

        //put <alpha, mh(rv)> into table vht
        for(String v : mht.keySet()){
            // t3 = G3(ke, cv||v)
            String cvLabel = cvName + "||" + v;
            byte[] t3 = strategyG3.hash(cvLabel, strFunnel);
            BigInteger u = BigInteger.probablePrime(COUNTER_SIZE, secureRandom);
//            BigInteger u = BigInteger.ONE;
            uct.put(cvLabel, u);

            // alpha = H4(t3, u)
            byte[] alpha = strategyH4.hash(Bytes.concat(t3, u.toByteArray()), byteArrayFunnel);
            String strAlpha = ApacheBase64Util.encode2String(alpha);


            MultiSetHash mh = mht.get(v);
            String strMh = ApacheBase64Util.encode2String(mh.getBytes());
            vht.put(strAlpha, strMh);
        }
        mht = null;

        // sort a value list of cv, to help get proof in range search model
        Set<Integer> allCvVals = new HashSet<Integer> ();
        for(String v : cvVals){
            allCvVals.add(Integer.parseInt(v));
        }
        List<Integer> orderList = new ArrayList<>(allCvVals);
        Collections.sort(orderList);
        cvt.put(cvName, orderList);
    }


    /** create a get token*/
    public GetToken getToken(String cvName, String cvVal){
        // t3 = G3(ke, cv||v)
        String cvLabel = cvName + "||" + cvVal;
        byte[] t3 = strategyG3.hash(cvLabel, strFunnel);
        BigInteger u = uct.get(cvLabel);
        GetToken getToken = GetToken.create(t3, u.toByteArray());
        return getToken;
    }


    /** create list of get token for range search model*/
    public List<GetToken> getToken(String cvName, String cvVal, boolean isSmaller){
        List<GetToken> tokens = new ArrayList<>();
        List<Integer> orderList = this.cvt.get(cvName);
        for(Integer v : orderList){
            if((isSmaller && v < Integer.parseInt(cvVal))
                    || (!isSmaller && v > Integer.parseInt(cvVal))){
                GetToken getToken = getToken(cvName, Integer.toString(v));
                tokens.add(getToken);
//                System.out.println("AFFIRMVHT.getToken: orderList size: " + orderList.size());
            }
        }
        return tokens;
    }




    /** get multiset hash */
    public MultiSetHash getMH(GetToken token){
        byte[] t3 = token.getT3();
        BigInteger u = new BigInteger(token.getU());

        //alpha
        byte[] alpha = strategyH4.hash(Bytes.concat(t3, u.toByteArray()), byteArrayFunnel);
        String strAlpha = ApacheBase64Util.encode2String(alpha);

        List<MultiSetHash> mhList = new LinkedList<>();

        while (vht.containsKey(strAlpha)){
            String strMh = vht.get(strAlpha);
            byte[] mhash = ApacheBase64Util.decode(strMh);
            MultiSetHash multiSetHash = MultiSetHash.create(mhash, strFunnel, strategyMH);
            mhList.add(multiSetHash);

            //u = TPpk(u) trapdoor permutation
            //is not being here now
            u = u.add(BigInteger.ONE);
            alpha = strategyH4.hash(Bytes.concat(t3, u.toByteArray()), byteArrayFunnel);
            strAlpha = ApacheBase64Util.encode2String(alpha);
        }

        //return
        if(mhList.size() == 0)
            return null;
        if(mhList.size() == 1)
            return mhList.get(0);
        //merge
        MultiSetHash mergedHash = mhList.get(0);
        mhList.remove(0);
        mergedHash.merge(mhList);
        return mergedHash;
    }

    /** get multiset hash */
    public MultiSetHash getMH(List<GetToken> tokens){
        if(tokens == null || tokens.size() == 0)
            return null;

        Iterator<GetToken> iterator = tokens.iterator();
        MultiSetHash mergedHash = getMH(iterator.next());
        List<MultiSetHash> mhs = new LinkedList<>();
        while (iterator.hasNext()) {
            mhs.add(getMH(iterator.next()));
        }
        mergedHash.merge(mhs);
        return mergedHash;
    }

    /** get multiset hash in string */
    public String getStrMH(GetToken token){
        MultiSetHash multiSetHash = getMH(token);
        return ApacheBase64Util.encode2String(multiSetHash.getBytes());
    }

    /** verify the search result */
    public boolean verify(List<String> rs, String crName, List<String> crVals, byte[] mhash){
        if(rs.size() != crVals.size())
            throw new IllegalArgumentException("rs size " + rs.size() + " is not equal to crVals size " + crVals.size());
        int len = rs.size();

        // multiset hash of result
        MultiSetHash<String> multiSetHash = null;
        for(int i = 0; i < len; i++){
            String r = rs.get(i);
            String crVal = crVals.get(i);
            String rv = r + "||" + crName + "||" + crVal;

            // multiset hash and add
            if(multiSetHash == null)
                multiSetHash = MultiSetHash.create(strFunnel, strategyMH);
            else
                multiSetHash.add(rv);
        }

        //compare with proof
        byte[] proof = multiSetHash.getBytes();
        for(int i = 0; i < proof.length; i++){
            if(proof[i] != mhash[i])
                return false;
        }
        return true;
    }


    /** create a new AFFIRMVHT with key and default hash strategy*/
    public static AFFIRMVHT create(String mykey){
        byte[] ke = (mykey + "ke").getBytes();
        byte[] kh = (mykey + "kh").getBytes();;
        return create(ke, kh,
                new MultiSetHashStrategies.HMACSHA256(ke),
                new MultiSetHashStrategies.SHA256(),
                new MultiSetHashStrategies.HMACSHA256(kh));
    }



    /** create a new AFFIRMVHT with key*/
    public static AFFIRMVHT create(byte[] ke, byte[] kh,
                                   MultiSetHashStrategies.HASH_FUNCTION_WITHKEY strategyG3,
                                   MultiSetHashStrategies.HASH_FUNCTION strategyH4,
                                   MultiSetHashStrategies.HASH_FUNCTION_WITHKEY strategyMH){
        Map<String, String> vht = new HashMap<>();
        return new AFFIRMVHT(ke, kh, vht, strategyG3, strategyH4, strategyMH);
    }


}
