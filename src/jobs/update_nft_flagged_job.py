import random
import time
from typing import Union

from multithread_processing.base_job import BaseJob
from multithread_processing.executors.batch_work_executor import BatchWorkExecutor

from src.databases.dex_nft_manager_db import NFTMongoDB
from src.utils.logger_utils import get_logger

logger = get_logger('Update Wallet Job')


class UpdateNFTFlaggedJob(BaseJob):
    def __init__(self, batch_size, max_workers, db: NFTMongoDB, nfts_batch):
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)

        self._db = db
        # self._graph = graph
        self.nfts_batch = nfts_batch

    def _start(self):
        self.klg_allowable_keys = ['tokenId', 'nftManagerAddress', 'flagged', 'lastUpdatedAt']


    def _end(self):
        self.batch_work_executor.shutdown()

    def _export(self):
        self.batch_work_executor.execute(
            self.nfts_batch,
            self._export_batch,
            total_items=len(self.nfts_batch)
        )

    def _export_batch(self, nfts_batch):
        for batch in nfts_batch:
            time.sleep(random.random())
            start_time = time.time()

            # Special update wallet flagged or credit score
            # klg_data = []
            # for doc in batch:
            #     info = dict(filter(lambda x: x[0] in self.klg_allowable_keys, doc.items()))
            #     info['_key'] = f'{doc["chainId"]}_{doc["nftManagerAddress"]}_{doc["tokenId"]}'
            #     klg_data.append(info)

            self._db.update_nfts(batch)
            # if self._graph:
            #     self._graph.update_wallets(klg_data)
            logger.info(f'Flag {len(batch)} wallets takes {time.time() - start_time} seconds')