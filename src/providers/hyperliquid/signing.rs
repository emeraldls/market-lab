use anyhow::{Context, Result, bail};
use k256::ecdsa::SigningKey;
use rand_core::OsRng;
use serde::Serialize;
use sha3::{Digest, Keccak256};

use super::HyperliquidNetwork;

const ZERO_ADDRESS: [u8; 20] = [0; 20];
const SIGNATURE_CHAIN_ID: u64 = 421_614;

#[derive(Clone)]
pub struct HyperliquidWallet {
    key: SigningKey,
}

#[derive(Clone, Debug, Serialize)]
pub struct WireSignature {
    pub r: String,
    pub s: String,
    pub v: u8,
}

impl HyperliquidWallet {
    pub fn random() -> Self {
        Self {
            key: SigningKey::random(&mut OsRng),
        }
    }

    pub fn from_private_key(value: &str) -> Result<Self> {
        let value = value.trim().strip_prefix("0x").unwrap_or(value.trim());
        let bytes = hex::decode(value).context("private key is not hexadecimal")?;
        if bytes.len() != 32 {
            bail!("private key must contain exactly 32 bytes");
        }
        let key = SigningKey::from_slice(&bytes).context("private key is outside secp256k1")?;
        Ok(Self { key })
    }

    pub fn private_key_hex(&self) -> String {
        format!("0x{}", hex::encode(self.key.to_bytes()))
    }

    pub fn address_bytes(&self) -> [u8; 20] {
        let encoded = self.key.verifying_key().to_encoded_point(false);
        let hash = keccak(&encoded.as_bytes()[1..]);
        let mut address = [0_u8; 20];
        address.copy_from_slice(&hash[12..]);
        address
    }

    pub fn address(&self) -> String {
        format!("0x{}", hex::encode(self.address_bytes()))
    }

    pub fn sign_l1_action<T: Serialize>(
        &self,
        action: &T,
        nonce: u64,
        network: HyperliquidNetwork,
    ) -> Result<WireSignature> {
        let mut bytes = rmp_serde::to_vec_named(action)
            .context("failed to encode Hyperliquid action for signing")?;
        bytes.extend(nonce.to_be_bytes());
        // Market Lab does not submit for vaults or subaccounts.
        bytes.push(0);
        let connection_id = keccak(bytes);
        let digest = typed_data_digest(
            exchange_domain_separator(),
            agent_struct_hash(network.signature_source(), connection_id),
        );
        self.sign_hash(digest)
    }

    pub fn sign_approve_agent(
        &self,
        agent_address: [u8; 20],
        agent_name: &str,
        nonce: u64,
        network: HyperliquidNetwork,
    ) -> Result<WireSignature> {
        let digest = typed_data_digest(
            transaction_domain_separator(SIGNATURE_CHAIN_ID),
            approve_agent_struct_hash(network.approval_chain(), agent_address, agent_name, nonce),
        );
        self.sign_hash(digest)
    }

    fn sign_hash(&self, hash: [u8; 32]) -> Result<WireSignature> {
        let (signature, recovery_id) = self
            .key
            .sign_prehash_recoverable(&hash)
            .context("failed to sign Hyperliquid action")?;
        let bytes = signature.to_bytes();
        Ok(WireSignature {
            r: format!("0x{}", hex::encode(&bytes[..32])),
            s: format!("0x{}", hex::encode(&bytes[32..])),
            v: u8::from(recovery_id) + 27,
        })
    }
}

pub fn canonical_address(value: &str) -> Result<String> {
    let stripped = value.trim().strip_prefix("0x").unwrap_or(value.trim());
    let bytes = hex::decode(stripped).context("address is not hexadecimal")?;
    if bytes.len() != 20 {
        bail!("address must contain exactly 20 bytes");
    }
    Ok(format!("0x{}", hex::encode(bytes)))
}

fn exchange_domain_separator() -> [u8; 32] {
    domain_separator("Exchange", 1_337)
}

fn transaction_domain_separator(chain_id: u64) -> [u8; 32] {
    domain_separator("HyperliquidSignTransaction", chain_id)
}

fn domain_separator(name: &str, chain_id: u64) -> [u8; 32] {
    let type_hash = keccak(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    keccak(abi_words([
        type_hash,
        keccak(name.as_bytes()),
        keccak(b"1"),
        uint_word(chain_id),
        address_word(ZERO_ADDRESS),
    ]))
}

fn agent_struct_hash(source: &str, connection_id: [u8; 32]) -> [u8; 32] {
    keccak(abi_words([
        keccak(b"Agent(string source,bytes32 connectionId)"),
        keccak(source.as_bytes()),
        connection_id,
    ]))
}

fn approve_agent_struct_hash(
    chain: &str,
    agent_address: [u8; 20],
    agent_name: &str,
    nonce: u64,
) -> [u8; 32] {
    keccak(abi_words([
        keccak(b"HyperliquidTransaction:ApproveAgent(string hyperliquidChain,address agentAddress,string agentName,uint64 nonce)"),
        keccak(chain.as_bytes()),
        address_word(agent_address),
        keccak(agent_name.as_bytes()),
        uint_word(nonce),
    ]))
}

fn typed_data_digest(domain: [u8; 32], structure: [u8; 32]) -> [u8; 32] {
    let mut bytes = Vec::with_capacity(66);
    bytes.extend([0x19, 0x01]);
    bytes.extend(domain);
    bytes.extend(structure);
    keccak(bytes)
}

fn address_word(address: [u8; 20]) -> [u8; 32] {
    let mut word = [0_u8; 32];
    word[12..].copy_from_slice(&address);
    word
}

fn uint_word(value: u64) -> [u8; 32] {
    let mut word = [0_u8; 32];
    word[24..].copy_from_slice(&value.to_be_bytes());
    word
}

fn abi_words<const N: usize>(words: [[u8; 32]; N]) -> Vec<u8> {
    words.into_iter().flatten().collect()
}

fn keccak(bytes: impl AsRef<[u8]>) -> [u8; 32] {
    Keccak256::digest(bytes.as_ref()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_canonical_ethereum_address() {
        let wallet = HyperliquidWallet::from_private_key(
            "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e",
        )
        .expect("wallet parses");
        assert_eq!(wallet.address().len(), 42);
        assert_eq!(
            canonical_address(&wallet.address()).expect("address"),
            wallet.address()
        );
    }

    #[test]
    fn l1_signature_matches_the_official_sdk_vector() {
        let wallet = HyperliquidWallet::from_private_key(
            "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e",
        )
        .expect("wallet parses");
        let connection_id =
            hex::decode("de6c4037798a4434ca03cd05f00e3b803126221375cd1e7eaaaf041768be06eb")
                .expect("fixture hash");
        let mut connection = [0_u8; 32];
        connection.copy_from_slice(&connection_id);
        let digest = typed_data_digest(
            exchange_domain_separator(),
            agent_struct_hash("b", connection),
        );
        let signature = wallet.sign_hash(digest).expect("sign");
        let encoded = format!(
            "{}{}{:02x}",
            signature.r.trim_start_matches("0x"),
            signature.s.trim_start_matches("0x"),
            signature.v
        );
        assert_eq!(
            encoded,
            "1713c0fc661b792a50e8ffdd59b637b1ed172d9a3aa4d801d9d88646710fb74b33959f4d075a7ccbec9f2374a6da21ffa4448d58d0413a0d335775f680a881431c"
        );
    }

    #[test]
    fn l1_signature_domain_separates_mainnet_and_testnet() {
        let wallet = HyperliquidWallet::from_private_key(
            "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e",
        )
        .expect("wallet parses");
        let action = serde_json::json!({
            "type": "cancel",
            "cancels": [{ "a": 3, "o": 42 }]
        });

        let mainnet = wallet
            .sign_l1_action(&action, 1_713_825_891_591, HyperliquidNetwork::Mainnet)
            .expect("mainnet action signs");
        let testnet = wallet
            .sign_l1_action(&action, 1_713_825_891_591, HyperliquidNetwork::Testnet)
            .expect("testnet action signs");

        assert_ne!(mainnet.r, testnet.r);
        assert_ne!(mainnet.s, testnet.s);
    }

    #[test]
    fn api_wallet_name_is_covered_by_the_approval_signature() {
        let wallet = HyperliquidWallet::from_private_key(
            "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e",
        )
        .expect("wallet parses");
        let agent = [1_u8; 20];
        let named = wallet
            .sign_approve_agent(agent, "marketlab", 1, HyperliquidNetwork::Testnet)
            .expect("named agent signs");
        let other = wallet
            .sign_approve_agent(agent, "another-app", 1, HyperliquidNetwork::Testnet)
            .expect("other agent signs");
        assert_ne!(named.r, other.r);
        assert_ne!(named.s, other.s);
    }

    #[test]
    fn api_wallet_approval_separates_mainnet_and_testnet() {
        let wallet = HyperliquidWallet::from_private_key(
            "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e",
        )
        .expect("wallet parses");
        let agent = [1_u8; 20];
        let mainnet = wallet
            .sign_approve_agent(agent, "marketlab", 1, HyperliquidNetwork::Mainnet)
            .expect("mainnet agent signs");
        let testnet = wallet
            .sign_approve_agent(agent, "marketlab", 1, HyperliquidNetwork::Testnet)
            .expect("testnet agent signs");
        assert_ne!(mainnet.r, testnet.r);
        assert_ne!(mainnet.s, testnet.s);
    }
}
