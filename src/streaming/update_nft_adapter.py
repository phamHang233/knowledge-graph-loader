import time

from src.constants.network_constants import Chains
from src.constants.time_constants import TimeConstants
from src.databases.blockchain_etl import BlockchainETL
from src.exporters import MongoDBExporter
from src.jobs.update_nft_job import UpdateNftInfoJob
from src.utils.file_utils import write_last_time_running_logs
from src.utils.logger_utils import get_logger

logger = get_logger('UPdate NFT Info Adapter')


class UpdateNftInfoAdapter:
    def __init__(self, importer: BlockchainETL, exporter: MongoDBExporter, collector_id="streaming_collector",
                 chain_id=Chains.bsc, batch_size=4, max_workers=8):
        self.collector_id = collector_id

        self.chain_id = chain_id

        self.batch_size = batch_size
        self.max_workers = max_workers

        self._exporter = exporter
        self._importer = importer

    def switch_provider(self):
        # Switch provider
        pass

    def get_current_block_number(self):
        return self._importer.get_last_block_number(self.collector_id)

    def enrich_all(self, start_block=0, end_block=0):
        start = time.time()
        logger.info(f"Start enrich block {start_block} - {end_block} ")
        self.enrich_data(start_block, end_block)
        end = time.time()
        logger.info(f"Enrich block {start_block} - {end_block} take {end - start}")

    def enrich_data(self, start_block, end_block):
        job = UpdateNftInfoJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            importer=self._importer,
            exporter=self._exporter,
            chain_id=self.chain_id,
        )
        job.run()

        write_last_time_running_logs(
            stream_name=f'{self.__class__.__name__}_{self.chain_id}',
            timestamp=int(time.time()),
            threshold=TimeConstants.MINUTES_15
        )
