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
    cursor = _dex_db.get_top_pairs(chain_id=chain_id, project='uniswap-v3')
    pairs_info = {doc['address']: doc for doc in cursor}

    result = []
    result_lock = threading.Lock()  # Create a lock to protect shared data

    threads = []
    cnt = 0

    for pair_address, pair_info in pairs_info.items():
        thread = threading.Thread(target=process_pair, args=(
            chain_id, pair_address, pair_info, start_timestamp, end_timestamp, result))
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
        logger.exception(f"Error processing pair {pair_address}: {e}")

def cal_pool(pool_address):
    _db = NFTMongoDB()
    _exporter = NFTMongoDBExporter(_db)
    _dex_db = MongoDBDex()
    # project = _dex_db.get_project('uniswap-v3', chain_id=chain_id)
    # top_pairs = project['topPairs']

    # end_timestamp =
    start_timestamp = int(time.time()) - 60 * 24 * 3600
    end_timestamp = int(time.time()) -  30 * 24 * 3600
    pair_info = _dex_db.get_pairs_with_addresses('0x1', [pool_address])[0]
    result = []
    process_pair('0x1', pool_address, pair_info,start_timestamp, end_timestamp, result)
    _exporter.export_pairs(result)

if __name__ == '__main__':
    # cal_pool("0xCBCdF9626bC03E24f779434178A73a0B4bad62eD")
    # cal_pool("0x517F9dD285e75b599234F7221227339478d0FcC8")
    cal_pool("0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640")
    # cal_pool("0x0d4a11d5EEaaC28EC3F61d100daF4d40471f1852")
    # cal_pool("0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc")
    # cal_pool("0x4e68Ccd3E89f51C3074ca5072bbAC773960dFa36")
    # cal_pool("0xc63B0708E2F7e69CB8A1df0e1389A98C35A76D52")
    # cal_pool("0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0")
    # cal_pool("0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8")
    # cal_pool("0x5777d92f208679DB4b9778590Fa3CAB3aC9e2168")
    # cal_pool("0x7A415B19932c0105c82FDB6b720bb01B0CC2CAe3")
    # cal_pool("0x11b815efB8f581194ae79006d24E0d814B7697F6")
'''

the best apr in generation 4st: 1.1138073477089587 - 68.05022864484285
the best range in generation 4st: [19373, 19483]
'''