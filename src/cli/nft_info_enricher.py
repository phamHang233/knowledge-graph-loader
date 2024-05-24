import os
import time

import click
from cli_scheduler.scheduler_job import scheduler_format

from src.constants.blockchain_etl_constants import DBPrefix
from src.constants.network_constants import Chains, Networks
from src.databases.dex_nft_manager_db import NFTMongoDB
from src.databases.mongodb_klg import MongoDB
from src.exporters import create_entity_db_and_exporter, ExporterType
from src.exporters.nft_mongodb_exporter import NFTMongoDBExporter
from src.jobs.nft_info_enricher_job import NFTInfoEnricherJob
from src.utils.logger_utils import get_logger

logger = get_logger('Elite wallet marker')

@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-c', '--chain', default='bsc', show_default=True, type=str, help='Network name example bsc or polygon')
@click.option('-b', '--batch-size', default=100, show_default=True, type=int, help='Batch size')
@click.option('--scheduler', default='^false@hourly', show_default=True, type=str, help=f'Scheduler with format "{scheduler_format}"')
def dex_nft_info_enricher(chain, batch_size, scheduler):
    chain = str(chain).lower()
    if chain not in Chains.mapping:
        raise click.BadOptionUsage("--chain", f"Chain {chain} is not support. Try {list(Chains.mapping.keys())}")
    chain_id = Chains.mapping[chain]
    db_prefix = DBPrefix.mapping.get(chain, '')

    _db = NFTMongoDB()
    _exporter = NFTMongoDBExporter(_db)

    logger.info(f'Connect to entity data: {_db.connection_url}')

    provider_uri = Networks.providers.get(Chains.names.get(chain_id))

    job = NFTInfoEnricherJob(
        _db=_db, _exporter=_exporter,
        scheduler=scheduler, batch_size=batch_size,
        chain_id=chain_id, provider_uri=provider_uri, db_prefix=db_prefix)

    job.run()
