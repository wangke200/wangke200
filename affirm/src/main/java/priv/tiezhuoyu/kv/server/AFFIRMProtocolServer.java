package priv.tiezhuoyu.kv.server;

import java.math.BigInteger;
import java.security.interfaces.RSAPublicKey;
import java.util.ArrayList;
import java.util.List;

import priv.tiezhuoyu.crypto.ApacheBase64Util;
import priv.tiezhuoyu.crypto.CryptoPrimitives;
import priv.tiezhuoyu.crypto.TrapdoorPermutation;

public class AFFIRMProtocolServer extends SEKVProtocolServer {
	public RSAPublicKey pk;

	public AFFIRMProtocolServer(KVStore kvAdapter, RSAPublicKey pk) {
		super(kvAdapter);
		this.pk = pk;
	}

	@Override
	public List<String> query(List<String> token) {
		byte[] t1 = ApacheBase64Util.decode(token.get(0));
		byte[] t2 = ApacheBase64Util.decode(token.get(1));
		BigInteger cnt = new BigInteger(ApacheBase64Util.decode(token.get(2)));

		List<String> Es = new ArrayList<>();
		while (true) {
			// alpha = H1(t1, cnt)
			byte[] t1Cnt = CryptoPrimitives.concat(t1, cnt.toByteArray());
			byte[] alpha = CryptoPrimitives.generateHmac(skH1, t1Cnt);

			// beta = E(ke, R) xor H2(t2, cnt)
			String betaBase64 = kvAdapter.get(ApacheBase64Util.encode2String(alpha));

			// if result is null, stop searching
			if (betaBase64 == null)
				break;
			byte[] beta = ApacheBase64Util.decode(betaBase64);

			// E(ke, R)= beta xor H2(t2, cnt)
			byte[] t2Cnt = CryptoPrimitives.concat(t2, cnt.toByteArray());
			byte[] betaMask = CryptoPrimitives.generateHmac(skH2, t2Cnt);
			for (int i = 0; i < beta.length; i++)
				beta[i] = (byte) (beta[i] ^ betaMask[i % betaMask.length]);

			Es.add(ApacheBase64Util.encode2String(beta));

			cnt = TrapdoorPermutation.tP(pk, cnt);
		}
		// if no result
		if (Es.size() == 0)
			Es.add(KVStore.NULL);
		return Es;
	}
}
