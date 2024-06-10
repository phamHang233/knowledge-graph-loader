import click

from src.constants.blockchain_etl_constants import DBPrefix
from src.constants.network_constants import Chains
from src.databases.dex_nft_manager_db import NFTMongoDB
from src.exporters.nft_mongodb_exporter import NFTMongoDBExporter
from src.streaming.update_nft_adapter import UpdateNftInfoAdapter
from src.streaming.streamer import Streamer
from src.utils.logger_utils import get_logger

logger = get_logger('Synced Wallet Stream')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-l', '--last-synced-block-file', default='last_synced_block.txt', show_default=True, type=str, help='')
@click.option('--lag', default=0, show_default=True, type=int, help='The number of blocks to lag behind the network.')
@click.option('-s', '--start-block', default=None, show_default=True, type=int, help='Start block')
@click.option('-e', '--end-block', type=int, default=None, show_default=True, help='End block')
@click.option('-B', '--block-batch-size', default=180, show_default=True, type=int,
              help='How many blocks to batch in single sync round')
@click.option('-b', '--batch-size', default=30, show_default=True, type=int,
              help='How many blocks to batch in a worker')
@click.option('--pid-file', default=None, show_default=True, type=str, help='pid file')
@click.option('-c', '--chain', default='bsc', show_default=True, type=str,
              help='network name example bsc or polygon')
@click.option('--stream-id', default=None, show_default=True, type=str, help='streamer id')
@click.option('--collector-id', default="streaming_collector", show_default=True, type=str, help='collector id')
def update_nft_info_stream(
        last_synced_block_file, lag, start_block, end_block, block_batch_size, batch_size, pid_file,
        chain, collector_id="", stream_id=None
):
    """Streaming load transactions to graph. """

    chain = str(chain).lower()
    if chain not in Chains.mapping:
        raise click.BadOptionUsage("--chain", f"Chain {chain} is not support")
    chain_id = Chains.mapping[chain]
    db_prefix = DBPrefix.mapping.get(chain, '')
    logger.info(f'Streaming with chain {chain} - prefix: {db_prefix}')

    _db = NFTMongoDB(db_prefix=db_prefix)
    _exporter = NFTMongoDBExporter(_db)
    logger.info(f'Connect to graph: {_db.connection_url}')

    dp_collector_id = f"{db_prefix}-{collector_id}"
    streamer_adapter = UpdateNftInfoAdapter(
        exporter=_exporter,
        importer=_db,
        collector_id=dp_collector_id,
        chain_id=chain_id,
        batch_size=batch_size,
        max_workers=8
    )
    streamer = Streamer(
        blockchain_streamer_adapter=streamer_adapter,
        exporter=_exporter,
        last_synced_block_file=last_synced_block_file,
        lag=lag,
        start_block=start_block,
        end_block=end_block,
        block_batch_size=block_batch_size,
        pid_file=pid_file,
        stream_id=stream_id
    )
    streamer.stream()
