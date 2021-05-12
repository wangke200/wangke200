package priv.tiezhuoyu.kv.server;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;

import priv.tiezhuoyu.crypto.ApacheBase64Util;
import priv.tiezhuoyu.crypto.CryptoPrimitives;

public class SEKVProtocolServer extends PlaintextKVProtocolServer {
	protected byte[] skH1, skH2;

	public SEKVProtocolServer(KVStore kvAdapter) {
		super(kvAdapter);
		byte[] tmpkey;
		Digest sha256 = new SHA256Digest();

		// skH1
		tmpkey = ("skH1").getBytes();
		sha256.update(tmpkey, 0, tmpkey.length);
		skH1 = new byte[sha256.getDigestSize()];
		sha256.doFinal(skH1, 0);

		// skH2
		tmpkey = ("skH2").getBytes();
		sha256.update(tmpkey, 0, tmpkey.length);
		skH2 = new byte[sha256.getDigestSize()];
		sha256.doFinal(skH2, 0);
	}

	@Override
	public List<String> query(List<String> token) {
		int cnt = 0;
		byte[] t1 = ApacheBase64Util.decode(token.get(0));
		byte[] t2 = ApacheBase64Util.decode(token.get(1));
		List<String> Es = new ArrayList<>();
		while (true) {
			// alpha = H1(t1, cnt)
			byte[] t1Cnt = CryptoPrimitives.concat(t1, Integer.toString(cnt).getBytes());
			byte[] alpha;
			alpha = CryptoPrimitives.generateHmac(skH1, t1Cnt);

			// beta = E(ke, R) xor H2(t2, cnt)
			String betaBase64 = kvAdapter.get(ApacheBase64Util.encode2String(alpha));
			if (betaBase64 == null)
				break;
			byte[] beta = ApacheBase64Util.decode(betaBase64);

			// E(ke, R)= beta xor H2(t2, cnt)
			byte[] t2Cnt = CryptoPrimitives.concat(t2, Integer.toString(cnt).getBytes());
			byte[] betaMask = CryptoPrimitives.generateHmac(skH2, t2Cnt);
			for (int i = 0; i < beta.length; i++)
				beta[i] = (byte) (beta[i] ^ betaMask[i % betaMask.length]);

			Es.add(ApacheBase64Util.encode2String(beta));

			cnt++;
		}
		// if no result
		if (Es.size() == 0)
			Es.add(KVStore.NULL);
		return Es;
	}
}
