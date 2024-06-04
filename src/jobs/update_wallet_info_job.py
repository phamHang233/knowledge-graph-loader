import time
from typing import Dict

from multithread_processing.base_job import BaseJob

from src.models.wallet import Wallet
from src.utils.logger_utils import get_logger

logger = get_logger('Update Wallet Info Enricher Job')

class UpdateWalletInfoJob(BaseJob):
    def __init__(
            self, _db, _exporter, max_workers, batch_size, wallets_batch,
            chain_id, provider_uri, db_prefix, n_cpu=1, cpu=1, ):
        self.n_cpu = n_cpu
        self.cpu = cpu - 1

        self._klg_db = _db
        self._exporter = _exporter
        self.db_prefix = db_prefix

        self.batch_size = batch_size
        self.number_of_wallets_batch = wallets_batch

        self.chain_id = chain_id
        # self.state_querier = StateQueryService(provider_uri)

        work_iterable = self._get_work_iterable()
        super().__init__(work_iterable, batch_size, max_workers)

    def _get_work_iterable(self):
        work_iterable = [idx for idx in range(1, self.number_of_wallets_batch + 1) if
                         (idx - 1) % self.n_cpu == self.cpu]
        return work_iterable

    # def _start(self):


    def _execute_batch(self,wallets_batch_indicates):
        for batch_idx in wallets_batch_indicates:
            update_wallets: Dict[str, Wallet] = {}
            try:
                start_timestamp  = time.time()
                cursor = self._klg_db.get_wallets(chain_id='0x1', batch_idx=batch_idx)
                nft_ids = [doc['nfts'].keys() for doc in cursor]
                nft_cursor = self._klg_db.get_nfts_by_keys(nft_ids)
                nft_infos = {doc["_id"]: doc for doc in nft_cursor}
                for doc in cursor:
                    total_liquidity = 0
                    address = doc['address']
                    wallet = Wallet(address=address, chain_id=self.chain_id)
                    wallet.from_dict(doc)
                    nfts = doc['nfts']
                    for nft_id in nfts:
                        nft_info = nft_infos.get(nft_id)
                        if not nft_info:
                            nfts.remove(nft_id)
                            continue
                        liquidity = nft_info['liquidity']
                        apr = nft_info['apr']
                        wallet.apr = (liquidity * apr + total_liquidity * wallet.apr) / (total_liquidity + liquidity)
                        wallet.pnl += nft_info['PnL']
                    update_wallets[address] = wallet
                self._export(update_wallets)
                logger.info(f"Execute batch {batch_idx} toke {time.time() - start_timestamp}")

            except Exception as e:
                raise e

    def _export(self, updated_wallets: Dict[str, Wallet]):
        data = [w.to_dict() for _, w in updated_wallets.items()]
        if data:
            self._exporter.export_wallets(data, chain_id=self.chain_id)