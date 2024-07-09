import time
from typing import Dict

from multithread_processing.base_job import BaseJob

from src.models.wallet import Wallet
from src.utils.logger_utils import get_logger

logger = get_logger('Update Wallet Info Enricher Job')


class UpdateWalletInfoJob(BaseJob):
    def __init__(
            self, _db, _exporter, max_workers, batch_size, num_nft_flagged,
            chain_id, db_prefix, n_cpu=1, cpu=1, ):
        self.n_cpu = n_cpu
        self.cpu = cpu - 1

        self._klg_db = _db
        self._exporter = _exporter
        # self.db_prefix = db_prefix
        self.updated_wallets: Dict[str, Wallet] = {}
        self.batch_size = batch_size
        self.number_of_nfts_batch = num_nft_flagged
        self.chain_id = chain_id
        work_iterable = self._get_work_iterable()
        super().__init__(work_iterable, batch_size, max_workers)

    def _get_work_iterable(self):
        work_iterable = [idx for idx in range(1, self.number_of_nfts_batch + 1) if
                         (idx - 1) % self.n_cpu == self.cpu]
        return work_iterable

    # def _start(self):
    def _end(self):
        self._export()

    def _execute_batch(self, nfts_batch_indicates):
        for batch_idx in nfts_batch_indicates:
            # update_wallets: Dict[str, Wallet] = {}
            try:
                start_timestamp = time.time()
                cursor = self._klg_db.get_nfts_by_filter(_filter={'flagged': batch_idx, 'aprInMonth': {"$gt": 0}, "chainId": self.chain_id})

                for doc in cursor:
                    wallet_address = doc.get('wallet')
                    if wallet_address not in self.updated_wallets:
                        self.updated_wallets[wallet_address] = Wallet(address=wallet_address, chain_id=self.chain_id)
                    wallet = self.updated_wallets.get(wallet_address)

                    liquidity = float(doc['liquidity'])
                    apr_in_month = doc['aprInMonth']
                    wallet.apr = (wallet.apr * wallet.total_liquidity + apr_in_month * liquidity) / (
                                liquidity + wallet.total_liquidity)
                    wallet.total_liquidity += liquidity
                    wallet.pnl += doc['PnL']
                    wallet.nfts.append(doc["_id"])
                    wallet.total_asset += doc['assetsInUSD']

                    #
                    # # wallet.from_dict(doc)
                    # # nfts = doc['nfts']
                    # for nft_id in nfts:
                    #     nft_info = nft_infos.get(nft_id)
                    #     if not nft_info:
                    #         nfts.remove(nft_id)
                    #         continue
                    #     liquidity = nft_info['liquidity']
                    #     apr = nft_info['apr']
                    #     wallet.apr = (liquidity * apr + total_liquidity * wallet.apr) / (total_liquidity + liquidity)
                    #     wallet.pnl += nft_info['PnL']
                    # update_wallets[address] = wallet
                logger.info(f"Execute batch {batch_idx} toke {time.time() - start_timestamp}")

            except Exception as e:
                raise e

    def _export(self):
        data = [w.to_dict() for _, w in self.updated_wallets.items()]
        if data:
            self._exporter.export_wallets(data, chain_id = self.chain_id)

