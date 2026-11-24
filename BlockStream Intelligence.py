import time
from collections import deque
from datetime import datetime

import grpc
from jito_searcher_client import get_searcher_client
from jito_searcher_client.convert import versioned_tx_to_protobuf_packet
from jito_searcher_client.generated.bundle_pb2 import Bundle
from jito_searcher_client.generated.searcher_pb2 import GetTipAccountsRequest, SendBundleRequest
from solana.rpc.api import Client
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
from solders.transaction import VersionedTransaction
from solders.transaction_status import TransactionConfirmationStatus


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [jito] {msg}")


def connect(block_engine_host, auth_keypair_path):
    with open(auth_keypair_path) as f:
        kp = Keypair.from_json(f.read())
    log(f"auth to {block_engine_host} as {kp.pubkey()}")
    return get_searcher_client(block_engine_host, kp)


_tip_ring = deque()


def pick_tip_account(searcher):
    if not _tip_ring:
        accounts = searcher.GetTipAccounts(GetTipAccountsRequest()).accounts
        if not accounts:
            accounts = searcher.GetTipAccounts(GetTipAccountsRequest()).accounts
        if not accounts:
            raise RuntimeError("tip list empty")
        _tip_ring.extend(accounts)
    _tip_ring.rotate(-1)
    return Pubkey.from_string(_tip_ring[-1])


def make_tip_tx(payer, tip_account, lamports, blockhash):
    ix = transfer(TransferParams(from_pubkey=payer.pubkey(), to_pubkey=tip_account, lamports=lamports))
    msg = MessageV0.try_compile(payer.pubkey(), [ix], [], blockhash)
    return VersionedTransaction(msg, [payer])


def wait_for_sig(rpc, sig, timeout_s=60):
    start = time.time()
    while time.time() - start < timeout_s:
        statuses = rpc.get_signature_statuses([sig]).value
        st = statuses[0] if statuses else None
        if st is not None:
            if st.err is not None:
                return "failed"
            if st.confirmation_status in (TransactionConfirmationStatus.Confirmed, TransactionConfirmationStatus.Finalized):
                return "landed"
        time.sleep(1.5)
    return "dropped"


class JitoTracker:
    def __init__(self):
        self.attempts = []

    def add(self, uuid, sig, status):
        self.attempts.append((uuid, str(sig), status))

    def land_rate(self):
        if len(self.attempts) == 0:
            return 0.0
        landed = len([a for a in self.attempts if a[2] == "landed"])
        return landed / len(self.attempts) * 100

    def summary(self):
        counts = {}
        for _, _, status in self.attempts:
            counts[status] = counts.get(status, 0) + 1
        log(f"attempts={len(self.attempts)} {counts} land rate {self.land_rate():.1f}%")


def send_mint_bundle(searcher, rpc, payer, mint_txs, tip_lamports, tracker):
    tip_account = pick_tip_account(searcher)
    blockhash = rpc.get_latest_blockhash().value.blockhash
    tip_tx = make_tip_tx(payer, tip_account, tip_lamports, blockhash)
    txs = list(mint_txs) + [tip_tx]
    if len(txs) > 5:
        log(f"[mint] Too many txs for one bundle ({len(txs)}), skipping")
        return None

    packets = [versioned_tx_to_protobuf_packet(tx) for tx in txs]
    try:
        resp = searcher.SendBundle(SendBundleRequest(bundle=Bundle(header=None, packets=packets)))
    except grpc.RpcError as e:
        log(f"send failed: {e.code()} {e.details()}")
        tracker.add(None, tip_tx.signatures[0], "rejected")
        return None

    last_sig = txs[-1].signatures[0]
    log(f"[mint] Bundle sent {resp.uuid}, tip {tip_lamports} -> {tip_account}, watching {last_sig}")
    status = wait_for_sig(rpc, last_sig)
    tracker.add(resp.uuid, last_sig, status)
    log(f"[mint] Bundle {resp.uuid}: {status}")
    return status


def run_mint(block_engine_host, auth_keypair_path, rpc_url, payer, build_mint_txs, tries=5, tip_lamports=200_000):
    searcher = connect(block_engine_host, auth_keypair_path)
    rpc = Client(rpc_url)
    tracker = JitoTracker()
    for i in range(tries):
        log(f"[mint] Try {i + 1}/{tries}")
        mint_txs = build_mint_txs(rpc)
        status = send_mint_bundle(searcher, rpc, payer, mint_txs, tip_lamports, tracker)
        if status == "landed":
            log("[mint] Landed!")
            break
        tip_lamports = int(tip_lamports * 1.5)
        time.sleep(2)
    else:
        log(f"[mint] Gave up after {tries} tries")
    tracker.summary()
    return tracker
