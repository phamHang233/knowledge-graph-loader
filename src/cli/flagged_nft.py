import time

import click
from config import DexNFTManagerDBConfig
from src.databases.dex_nft_manager_db import NFTMongoDB
from src.utils.logger_utils import get_logger

logger = get_logger('Wallet Flag')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-g', '--graph', default=None, type=str,
              help='graph_db connection url example: bolt://localhost:7687@username:password')
@click.option('-c', '--chain-id', default=None, type=str, help='Chain id of network to calculate scores')
@click.option('-bs', '--batch-size', default=1000, show_default=True, type=int,
              help='Batch size to get and update nfts')
@click.option('-r', '--reset', default=False, show_default=True, type=bool, help='Reset flag')
@click.option('-l', '--loop', default=True, show_default=True, type=bool, help='Loop this action')
@click.option('-sl', '--sleep', default=86400, show_default=True, type=int, help='Sleep time')
def nft_flagged(graph, chain_id, batch_size, reset, loop, sleep):
    """Calculate all nfts credit score, update to knowledge graph."""
    if graph is None:
        graph = DexNFTManagerDBConfig.CONNECTION_URL
    _db = NFTMongoDB(connection_url=graph)

    while True:
        start_time = time.time()
        try:
            logger.info(f'Database connected: {graph.split("@")[-1]}')
            calculator = WalletsCreditScore(
                _db=_db
            )

            calculator.nft_flag(
                chain_id=chain_id,
                batch_size=batch_size,
                reset=reset
            )
            del calculator
            if not loop:
                break
        except Exception as ex:
            logger.exception(ex)

        sleep_time = max(sleep - (time.time() - start_time), 0)
        logger.info(f'Sleep {sleep} seconds')
        time.sleep(sleep_time)

    logger.info('Done')


class WalletsCreditScore:
    def __init__(self, _db: NFTMongoDB):
        self._db = _db

    def nft_flag(self, chain_id=None, batch_size=10000, reset=False):
        logger.info('Start Flagging')
        start_time = time.time()
        cnt = 0
        cursor = self._db.get_nft_with_flagged(batch_size=batch_size, chain_id=chain_id, reset=reset)
        logger.info(f'Create cursor and fetch first batch take {time.time() - start_time} seconds')

        flagged_state = self._db.get_nft_flagged_state(chain_id=chain_id)
        if flagged_state and not reset:
            batch_idx = flagged_state['batch_idx']
            n_nfts_current_batch = flagged_state['n_nfts_current_batch']
        else:
            batch_idx, n_nfts_current_batch = 1, 0

        updated_nfts = []

        try:
            for w in cursor:
                token_id = w.get('tokenId')
                nft_manager = w.get('nftManagerAddress')
                if not token_id or not nft_manager:
                    continue

                n_nfts_current_batch += 1
                updated = {'chainId': chain_id, 'tokenId': token_id, 'nftManagerAddress': nft_manager,
                           'flagged': batch_idx}

                updated_nfts.append(updated)
                cnt += 1
                if n_nfts_current_batch >= batch_size:
                    batch_idx += 1
                    self._db.update_nfts(updated_nfts)
                    n_nfts_current_batch = 0
                    updated_nfts = []

                    logger.info(f'[{batch_idx - 1}] Flag {cnt} nfts takes {time.time() - start_time} seconds')

        except Exception as ex:
            logger.exception(ex)

        flagged_state = {'batch_idx': batch_idx, 'n_nfts_current_batch': n_nfts_current_batch,
                         '_id': f'nfts_flagged_state_{chain_id}'}

        self._db.update_configs([flagged_state])

        if updated_nfts:
            self._db.update_nfts(updated_nfts)

        logger.info(f'Flag {cnt} nfts takes {time.time() - start_time} seconds')
