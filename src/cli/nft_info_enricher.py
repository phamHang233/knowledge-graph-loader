import os
import time

import click
from cli_scheduler.scheduler_job import scheduler_format

from src.constants.blockchain_etl_constants import DBPrefix
from src.constants.network_constants import Chains, Networks
from src.databases.mongodb_klg import MongoDB
from src.exporters import create_entity_db_and_exporter, ExporterType
from src.jobs.nft_info_enricher_job import NFTInfoEnricherJob
from src.utils.logger_utils import get_logger

logger = get_logger('Elite wallet marker')

@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('--exporter-type', default=ExporterType.MONGODB, show_default=True, type=str,
              help='Destination to export entity data')
@click.option('-c', '--chain', default='bsc', show_default=True, type=str, help='Network name example bsc or polygon')
@click.option('-b', '--batch-size', default=100, show_default=True, type=int, help='Batch size')
@click.option('--scheduler', default='^false@hourly', show_default=True, type=str, help=f'Scheduler with format "{scheduler_format}"')
def dex_nft_info_enricher(exporter_type, chain, batch_size, scheduler):
    chain = str(chain).lower()
    if chain not in Chains.mapping:
        raise click.BadOptionUsage("--chain", f"Chain {chain} is not support. Try {list(Chains.mapping.keys())}")
    chain_id = Chains.mapping[chain]
    db_prefix = DBPrefix.mapping.get(chain, '')

    _klg = MongoDB()
    _db, _exporter = create_entity_db_and_exporter(output=None, exporter_type=exporter_type)
    logger.info(f'Connect to entity data: {_db.connection_url}')

    provider_uri = Networks.providers.get(Chains.names.get(chain_id))

    job = NFTInfoEnricherJob(
        _db=_db, _exporter=_exporter,
        scheduler=scheduler, batch_size=batch_size,
        chain_id=chain_id, provider_uri=provider_uri, db_prefix=db_prefix)

    job.run()
