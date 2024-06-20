import threading
import time
from threading import Thread

import click

from job.algorthisms.genetic_algorithms import GeneticAlgorithms
from src.constants.blockchain_etl_constants import DBPrefix
from src.constants.network_constants import Chains
from src.databases.dex_nft_manager_db import NFTMongoDB
from src.databases.mongodb_dex import MongoDBDex
from src.exporters.nft_mongodb_exporter import NFTMongoDBExporter
from src.jobs.calculate_optimized_range_job import CalculateOptimizedRange
from src.utils.logger_utils import get_logger

logger = get_logger('NFT Info Enricher')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
# @click.option("-nc", "--n-cpu", default=1, show_default=True, type=int, help="Number of CPU")
@click.option('-c', '--chain', default='ethereum', show_default=True, type=str,
              help='Network name example bsc or polygon')
# @click.option("-cpu", default=1, show_default=True, type=int, help="CPU order")
# @click.option('-b', '--batch-size', default=1, show_default=True, type=int, help='NFT Batch size')
# @click.option('--scheduler', default='^false@hourly', show_default=True, type=str, help=f'Scheduler with format "{scheduler_format}"')
@click.option("-w", "--max-workers", default=5, show_default=True, type=int, help="The number of workers")
def calculate_optimized_price_range(chain, max_workers):
    chain = str(chain).lower()
    if chain not in Chains.mapping:
        raise click.BadOptionUsage("--chain", f"Chain {chain} is not support. Try {list(Chains.mapping.keys())}")
    chain_id = Chains.mapping[chain]

    _db = NFTMongoDB()
    _exporter = NFTMongoDBExporter(_db)
    _dex_db = MongoDBDex()
    # project = _dex_db.get_project('uniswap-v3', chain_id=chain_id)
    # top_pairs = project['topPairs']

    end_timestamp = int(time.time())
    start_timestamp = end_timestamp - 30 * 24 * 3600
    # pairs_address = [key.split("_")[1] for key in top_pairs]
    cursor = _dex_db.get_top_pairs(chain_id=chain_id, project='uniswap-v3')
    pairs_info = {doc['address']: doc for doc in cursor}

    # result = []
    # for pair_address, pair_info in pairs_info.items():
    #     try:
    #         # pair_address = key.split("_")[1]
    #         ga = GeneticAlgorithms(pool=pair_address, start_timestamp=start_timestamp,
    #                                end_timestamp=end_timestamp)
    #         best_apr, best_range = ga.process()
    #
    #         result.append({
    #             "_id": f"{chain_id}_{pair_address}",
    #             'address': pair_address,
    #             'token0': pair_info['tokens'][0]['address'],
    #             'token1': pair_info['tokens'][1]['address'],
    #             'bestAPR': best_apr,
    #             'range': best_range
    #         })
    #     except Exception as e:
    #         logger.exception(e)

    result = []
    result_lock = threading.Lock()  # Create a lock to protect shared data

    threads = []
    cnt = 0

    for pair_address, pair_info in pairs_info.items():
        thread = threading.Thread(target=process_pair, args=(
            chain_id, pair_address, pair_info, start_timestamp, end_timestamp, result_lock, result))
        thread.start()
        threads.append(thread)
        cnt += 1
        if cnt % max_workers == 0:

            for thread in threads:
                thread.join()  # Wait for all threads to finish
            _exporter.export_pairs(result)
            result = []
            threads = []
            cnt = 0

    # parallelize_processing(top_pairs=top_pairs,pairs_info=pairs_info,start_timestamp=start_timestamp,end_timestamp=end_timestamp, num_threads=max_workers)


def process_pair(chain_id, pair_address, pair_info, start_timestamp, end_timestamp, result):
    try:
        ga = GeneticAlgorithms(pool=pair_address, start_timestamp=start_timestamp,
                               end_timestamp=end_timestamp)
        best_apr, best_range = ga.process()

        result.append({
            "_id": f"{chain_id}_{pair_address}",
            'address': pair_address,
            'token0': pair_info['tokens'][0]['address'],
            'token1': pair_info['tokens'][1]['address'],
            'bestAPR': best_apr,
            'range': best_range
        })
    except Exception as e:
        print(f"Error processing pair {pair_address}: {e}")

def cal_pool(pool_address):
    _db = NFTMongoDB()
    _exporter = NFTMongoDBExporter(_db)
    _dex_db = MongoDBDex()
    # project = _dex_db.get_project('uniswap-v3', chain_id=chain_id)
    # top_pairs = project['topPairs']

    end_timestamp = int(time.time())
    start_timestamp = end_timestamp - 30 * 24 * 3600
    pair_info = _dex_db.get_pairs_with_addresses('0x1', [pool_address])[0]
    result = []
    process_pair('0x1', pool_address, pair_info,start_timestamp, end_timestamp, result)
    _exporter.export_pairs(result)

if __name__ == '__main__':
    cal_pool("0x7bea39867e4169dbe237d55c8242a8f2fcdcc387")
    cal_pool("0xe0554a476a092703abdb3ef35c80e0d76d32939f")