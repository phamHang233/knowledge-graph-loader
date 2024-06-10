import time

from query_state_lib.client.client_querier import ClientQuerier
from web3 import Web3

from job.artifacts.abis.pool_v3_abi import UNISWAP_V3_POOL_ABI
from job.artifacts.abis.uniswap_nft_manager_abi import UNISWAP_V3_NFT_MANAGER_ABI
from job.constants.network_constants import Networks
from job.databases.blockchain_etl import BlockchainETL
from job.databases.mongodb_klg import MongoDB
from job.services.batch_queries_service import decode_data_response, add_rpc_call
from job.utils.sqrt_price_math import get_fees

mongo_klg = MongoDB()
mongo_etl = BlockchainETL(db_prefix='ethereum')
client_querier = ClientQuerier(Networks.archive_node.get('ethereum'))
nft_manager_address = '0xC36442b4a4522E871399CD717aBDD847Ab11FE88'
chain_id = '0x1'
start_block = 19336607
end_block = 19343772
retry_exceptions=5
max_retries = 5
def get_wallets():
    wallets_add_liq = mongo_klg.get_wallets()
    token_pools = {}
    tokens_key = set()
    token_addresses = set()
    pool_infos = {}
    for wallet in wallets_add_liq:
        for pool_address, nfts_info in wallet['dexes'].items():
            tokens = nfts_info['tokens']
            token0 = list(tokens.keys())[0]
            token1 = list(tokens.keys())[1]
            if pool_address not in token_pools:
                token_pools[pool_address] = {}
                pool_infos[pool_address] = {
                    'token0': token0,
                    'token1': token1
                }

            for block_number, log in nfts_info['liquidityChangeLogs'].items():
                token_id = log['nftId']

                if start_block < int(block_number) < end_block and token_id not in token_pools[pool_address]:
                    tick_lower = nfts_info['nftTokens'][token_id]['tickLower']
                    tick_upper = nfts_info['nftTokens'][token_id]['tickUpper']
                    token_pools[pool_address][token_id] = {
                        'tickLower': tick_lower,
                        'tickUpper': tick_upper,
                        'liquidity': nfts_info['nftTokens'][token_id]['liquidity']
                    }
                    tokens_key.add(token_id)
                    token_addresses.add(token0)
                    token_addresses.add(token1)

    cursor = mongo_etl.get_collect_events_by_token_id(list(tokens_key))
    collected_fee = {}
    for event in cursor:
        amount0 = event['amount0']
        amount1 = event['amount1']
        token_id = event['tokenId']
        if token_id in tokens_key:
            if token_id not in collected_fee:
                collected_fee[token_id] = {
                    'amount0': 0,
                    'amount1': 0
                }

            collected_fee[token_id]['amount0'] += int(amount0)
            collected_fee[token_id]['amount1'] += int(amount1)
    list_call_id, list_rpc_call = [], []

    for pool_address, tokens in token_pools.items():
        if tokens and pool_address:
            checksum_pool_address = Web3.toChecksumAddress(pool_address)
            add_rpc_call(abi=UNISWAP_V3_POOL_ABI, contract_address=checksum_pool_address, block_number='latest',
                         fn_name="feeGrowthGlobal0X128", list_call_id=list_call_id, list_rpc_call=list_rpc_call)
            add_rpc_call(abi=UNISWAP_V3_POOL_ABI, contract_address=checksum_pool_address, block_number='latest',
                         fn_name="feeGrowthGlobal1X128", list_call_id=list_call_id, list_rpc_call=list_rpc_call)
            add_rpc_call(abi=UNISWAP_V3_POOL_ABI, contract_address=checksum_pool_address, block_number='latest',
                         fn_name="slot0", list_call_id=list_call_id, list_rpc_call=list_rpc_call)

            for token_id, token_info in tokens.items():
                tick_lower = token_info['tickLower']
                tick_upper = token_info['tickUpper']

                add_rpc_call(abi=UNISWAP_V3_POOL_ABI, contract_address=checksum_pool_address,
                             fn_name="ticks", fn_paras=tick_lower, list_call_id=list_call_id,
                             list_rpc_call=list_rpc_call, block_number='latest')
                add_rpc_call(abi=UNISWAP_V3_POOL_ABI, contract_address=checksum_pool_address,
                             fn_name="ticks", fn_paras=tick_upper, list_call_id=list_call_id, block_number='latest',
                             list_rpc_call=list_rpc_call)
                add_rpc_call(abi=UNISWAP_V3_NFT_MANAGER_ABI,
                             contract_address=Web3.toChecksumAddress(nft_manager_address),
                             fn_name="positions", fn_paras=int(token_id), list_call_id=list_call_id, block_number='latest',
                             list_rpc_call=list_rpc_call)

    response = client_querier.sent_batch_to_provider(list_rpc_call)
    filtered_list_call_id = []
    for call_id in list_call_id:
        if response[call_id].result:
            filtered_list_call_id.append(call_id)
    decoded_data = decode_data_response(response, filtered_list_call_id)

    uncollected_fee = {}
    for pool_address, tokens in token_pools.items():
        for token_id, token_info in tokens.items():
            tick_lower = token_info['tickLower']
            tick_upper = token_info['tickUpper']
            liquidity = token_info['liquidity']
            block_number = 'latest'
            tick = decoded_data.get(f"slot0_{pool_address}_{block_number}".lower())[1]
            fee_growth_global_0 = decoded_data.get(
                f'feeGrowthGlobal0X128_{pool_address}_{block_number}'.lower())
            fee_growth_global_1 = decoded_data.get(
                f'feeGrowthGlobal1X128_{pool_address}_{block_number}'.lower())
            fee_growth_0_low_x128 = decoded_data.get(f'ticks_{pool_address}_{tick_lower}_{block_number}'.lower())[2]
            fee_growth_1_low_x128 = decoded_data.get(f'ticks_{pool_address}_{tick_lower}_{block_number}'.lower())[3]
            fee_growth_0_hi_x128 = decoded_data.get(f'ticks_{pool_address}_{tick_upper}_{block_number}'.lower())[2]
            fee_growth_1_hi_x128 = decoded_data.get(f'ticks_{pool_address}_{tick_upper}_{block_number}'.lower())[3]
            fee_growth_inside_0_x128 = \
                decoded_data.get(f'positions_{nft_manager_address}_{token_id}_{block_number}'.lower())[8]
            fee_growth_inside_1_x128 = \
                decoded_data.get(f'positions_{nft_manager_address}_{token_id}_{block_number}'.lower())[9]
            token0_reward, token1_reward = get_fees(
                fee_growth_global_0=fee_growth_global_0,
                fee_growth_global_1=fee_growth_global_1,
                fee_growth_0_low=fee_growth_0_low_x128,
                fee_growth_1_low=fee_growth_1_low_x128,
                fee_growth_0_hi=fee_growth_0_hi_x128,
                fee_growth_1_hi=fee_growth_1_hi_x128,
                fee_growth_inside_0=fee_growth_inside_0_x128,
                fee_growth_inside_1=fee_growth_inside_1_x128,
                liquidity=liquidity, tick_lower=tick_lower,
                tick_upper=tick_upper, tick_current=tick)
            if token0_reward or token1_reward:
                uncollected_fee[token_id] = {
                    'amount0': token0_reward,
                    'amount1': token1_reward
                }

    token_addresses = [f'{chain_id}_{token}' for token in token_addresses]
    cursor = mongo_klg.get_tokens_by_keys(token_addresses)
    token_price = {doc['address']: {'priced': doc['price'], 'decimals': doc.get('decimals', 18)} for doc in cursor}
    max_fee = {}
    for pool_address, tokens in token_pools.items():
        current_max = {}
        token0 = pool_infos.get('token0')
        token1 = pool_infos.get('token1')
        for token_id, token_info in tokens.items():
            tick_lower = token_info['tickLower']
            tick_upper = token_info['tickUpper']
            amount0_collected = collected_fee.get(token_id, {}).get('amount0', 0)
            amount1_collected = collected_fee.get(token_id, {}).get('amount1', 0)
            amount0_uncollected = uncollected_fee[token_id]['amount0']
            amount1_uncollected = uncollected_fee[token_id]['amount1']
            amount0 = amount0_uncollected + amount0_collected
            amount1 = amount1_uncollected + amount1_collected
            value_in_usd = amount0 * token_price[token0]['price'] / 10 ** token_price[token0]['decimals'] \
                           + amount1 * token_price[token1]['price'] / 10 ** token_price[token1]['decimals']
            if value_in_usd > current_max['value']:
                current_max = {
                    'tickUpper': tick_upper,
                    'tickLower': tick_lower,
                    'value_in_usd': value_in_usd
                }
        max_fee[pool_address] = current_max

if __name__ == '__main__':
    get_wallets()
