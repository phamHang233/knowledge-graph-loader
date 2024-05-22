import time

import click

from src.constants.blockchain_etl_constants import DBPrefix
from src.constants.network_constants import Chains, Networks
from src.constants.time_constants import TimeConstants
from src.databases.blockchain_etl import BlockchainETL
from src.databases.mongodb_klg import MongoDB
from src.exporters import create_entity_db_and_exporter, ExporterType
from src.jobs.nft_info_enricher_job import NFTInfoEnricherJob
from src.utils.logger_utils import get_logger

logger = get_logger('Elite wallet marker')


@click.option('-nc', '--n-cpu', default=1, show_default=True, type=int, help='Number of CPU')
@click.option('-w', '--max-workers', default=4, show_default=True, type=int,
              help='Max workers to get and update wallets')
@click.option('-cpu', '--cpu', default=1, show_default=True, type=int, help='CPU order')
@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('--exporter-type', default=ExporterType.MONGODB, show_default=True, type=str,
              help='Destination to export entity data')
@click.option('-c', '--chain', default='bsc', show_default=True, type=str, help='Network name example bsc or polygon')
@click.option('-b', '--batch-size', default=100, show_default=True, type=int, help='Batch size')
@click.option('-s', '--start-time', default=None, type=int, help='start timestamp')
@click.option('-e', '--end-time', default=None, type=int, help='end timestamp')
@click.option('-i', '--interval', default=TimeConstants.A_DAY, type=int, help='Sleep time')
@click.option('-l', '--last-synced-file', default='last_synced.txt', show_default=True, type=str, help='')
@click.option('-m', '--monitor', default=False, show_default=True,
              type=bool, help='Monitor or not')
@click.option('--stream-id', default=None, show_default=True, type=str, help='streamer id')
def nft_info_enricher(exporter_type, chain, batch_size, interval, max_workers,
                      n_cpu, cpu, stream_id, monitor, start_time, end_time, last_synced_file):
    """Run job that mark top tokens holders & lending wallets as new elite wallets
    (Code-wise is same as wallets_enricher, only deviates at the step getting list of wallets)"""
    chain = str(chain).lower()
    if chain not in Chains.mapping:
        raise click.BadOptionUsage("--chain", f"Chain {chain} is not support. Try {list(Chains.mapping.keys())}")
    chain_id = Chains.mapping[chain]
    db_prefix = DBPrefix.mapping.get(chain, '')

    _klg = MongoDB()
    _etl_db = BlockchainETL(db_prefix=db_prefix)
    end_block = _etl_db.get_last_block_number()
    current_day_timestamp = int(int(time.time()) / 24 / 3600) * 24 * 3600
    before_30_days_block = _etl_db.get_block_by_timestamp(current_day_timestamp - 24 * 30 * 3600)[0]['number']
    _db, _exporter = create_entity_db_and_exporter(output=None, exporter_type=exporter_type)
    logger.info(f'Connect to entity data: {_db.connection_url}')
    provider_uri = Networks.providers.get(Chains.names.get(chain_id))

    job = NFTInfoEnricherJob(
        _db=_db, _exporter=_exporter, end_block=end_block, before_30_days_block=before_30_days_block,
        interval=interval, max_workers=max_workers, batch_size=batch_size,
        chain_id=chain_id, provider_uri=provider_uri,
        n_cpu=n_cpu, cpu=cpu, stream_id=stream_id, monitor=monitor,
        end_timestamp=end_time, start_timestamp=start_time, last_synced_file=last_synced_file)

    job.run()
