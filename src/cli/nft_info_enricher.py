import os
import time

import click
import requests
from cli_scheduler.scheduler_job import scheduler_format

from src.constants.blockchain_etl_constants import DBPrefix
from src.constants.network_constants import Chains, Networks
from src.databases.dex_nft_manager_db import NFTMongoDB
from src.exporters.nft_mongodb_exporter import NFTMongoDBExporter
from src.jobs.arbitrum_nft_info_enricher_job import NFTInfoEnricherJob
from src.utils.logger_utils import get_logger

logger = get_logger('NFT Info Enricher')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
# @click.option("-nc", "--n-cpu", default=1, show_default=True, type=int, help="Number of CPU")
@click.option('-c', '--chain', default='ethereum', show_default=True, type=str, help='Network name example bsc or polygon')
@click.option('--scheduler', default='^false@daily', show_default=True, type=str, help=f'Scheduler with format "{scheduler_format}"')

# @click.option("-cpu", default=1, show_default=True, type=int, help="CPU order")
# @click.option('-b', '--batch-size', default=1, show_default=True, type=int, help='NFT Batch size')
# @click.option('--scheduler', default='^false@hourly', show_default=True, type=str, help=f'Scheduler with format "{scheduler_format}"')
# @click.option("-w", "--max-workers", default=1, show_default=True, type=int, help="The number of workers")
def dex_nft_info_enricher(chain, scheduler):
    chain = str(chain).lower()
    if chain not in Chains.mapping:
        raise click.BadOptionUsage("--chain", f"Chain {chain} is not support. Try {list(Chains.mapping.keys())}")
    chain_id = Chains.mapping[chain]
    db_prefix = DBPrefix.mapping.get(chain, '')

    _db = NFTMongoDB()
    _exporter = NFTMongoDBExporter(_db)

    logger.info(f'Connect to entity data: {_db.connection_url}')

    provider_uri = Networks.archive_node.get(Chains.names[chain_id])

    flagged_state = _db.get_nft_flagged_state(chain_id=chain_id)
    nfts_batch = flagged_state["batch_idx"]

    job = NFTInfoEnricherJob(
        _db=_db, _exporter=_exporter, nfts_batch=nfts_batch, scheduler=scheduler,
        chain_id=chain_id, provider_uri=provider_uri, db_prefix=db_prefix)

    job.run()
