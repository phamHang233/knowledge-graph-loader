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
from src.jobs.update_wallet_info_job import UpdateWalletInfoJob
from src.utils.logger_utils import get_logger

logger = get_logger('Elite wallet marker')

@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option("-nc", "--n-cpu", default=1, show_default=True, type=int, help="Number of CPU")
@click.option('-c', '--chain', default='ethereum', show_default=True, type=str, help='Network name example bsc or polygon')
@click.option("-cpu", default=1, show_default=True, type=int, help="CPU order")
@click.option('-b', '--batch-size', default=1, show_default=True, type=int, help='NFT Batch size')
# @click.option('--scheduler', default='^false@hourly', show_default=True, type=str, help=f'Scheduler with format "{scheduler_format}"')
@click.option("-w", "--max-workers", default=1, show_default=True, type=int, help="The number of workers")
def dex_wallet_info_enricher(chain, batch_size, n_cpu, cpu, max_workers):
    chain = str(chain).lower()
    if chain not in Chains.mapping:
        raise click.BadOptionUsage("--chain", f"Chain {chain} is not support. Try {list(Chains.mapping.keys())}")
    chain_id = Chains.mapping[chain]
    db_prefix = DBPrefix.mapping.get(chain, '')

    _db = NFTMongoDB()
    _exporter = NFTMongoDBExporter(_db)

    logger.info(f'Connect to entity data: {_db.connection_url}')

    # provider_uri = Networks.archive_node.get(Chains.names[chain_id])

    num_flagged_state = _db.get_nft_flagged_state(chain_id=chain_id)["batch_idx"]

    job = UpdateWalletInfoJob(
        _db=_db, _exporter=_exporter, num_nft_flagged=num_flagged_state,
        batch_size=batch_size, n_cpu=n_cpu, cpu=cpu, max_workers=max_workers,
        chain_id=chain_id)

    job.run()
