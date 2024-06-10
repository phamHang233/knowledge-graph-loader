import os


class Networks:
    bsc = 'bsc'
    ethereum = 'ethereum'
    fantom = 'fantom'
    polygon = 'polygon'
    arbitrum = 'arbitrum'
    optimism = 'optimism'
    avalanche = 'avalanche'
    tron = 'tron'
    cronos = 'cronos'
    solana = 'solana'
    polkadot = 'polkadot'

    providers = {
        bsc: os.getenv('BSC_PROVIDER_URI', 'https://bsc-dataseed1.binance.org/'),
        ethereum: os.getenv('ETHEREUM_PROVIDER_URI', 'https://rpc.ankr.com/eth'),
        fantom: os.getenv('FANTOM_PROVIDER_URI', 'https://rpc.ftm.tools/'),
        polygon: os.getenv('POLYGON_PROVIDER_URI', 'https://polygon-rpc.com'),
        arbitrum: os.getenv('ARBITRUM_PROVIDER_URI', 'https://endpoints.omniatech.io/v1/arbitrum/one/public'),
        optimism: os.getenv('OPTIMISM_PROVIDER_URI', 'https://rpc.ankr.com/optimism'),
        avalanche: os.getenv('AVALANCHE_PROVIDER_URI', 'https://rpc.ankr.com/avalanche'),
        tron: os.getenv('TRON_PROVIDER_URI', 'https://rpc.ankr.com/tron_jsonrpc'),
        cronos: os.getenv('CRONOS_PROVIDER_URI', 'https://evm.cronos.org/'),
        solana: os.getenv('SOLANA_PROVIDER_URI',
                          'https://crimson-multi-putty.solana-mainnet.quiknode.pro/997174ce6ab5cc9d42cb037e931d18ae1a98346a/'),
        polkadot: os.getenv('POLKADOT_PROVIDER_URI',
                            'https://late-yolo-diagram.dot-mainnet.quiknode.pro/51a1aaf2372854dfd211fca3ab375e5451222be4/')
    }

    archive_node = {
        bsc: os.getenv('BSC_PROVIDER_ARCHIVE_URI', 'https://rpc.ankr.com/bsc'),
        ethereum: os.getenv('ETHEREUM_PROVIDER_ARCHIVE_URI', 'https://rpc.ankr.com/eth'),
        fantom: os.getenv('FANTOM_PROVIDER_ARCHIVE_URI', 'https://rpc.ankr.com/fantom'),
        polygon: os.getenv('POLYGON_PROVIDER_ARCHIVE_URI', 'https://rpc.ankr.com/polygon'),
        arbitrum: os.getenv('ARBITRUM_PROVIDER_ARCHIVE_URI', 'https://rpc.ankr.com/arbitrum'),
        optimism: os.getenv('OPTIMISM_PROVIDER_ARCHIVE_URI', 'https://rpc.ankr.com/optimism'),
        avalanche: os.getenv('AVALANCHE_PROVIDER_ARCHIVE_URI', 'https://rpc.ankr.com/avalanche'),
        tron: os.getenv('TRON_PROVIDER_ARCHIVE_URI', 'https://rpc.ankr.com/tron_jsonrpc'),
        cronos: os.getenv('CRONOS_PROVIDER_ARCHIVE_URI'),
        solana: os.getenv('SOLANA_PROVIDER_ARCHIVE_URI'),
        polkadot: os.getenv('POLKADOT_PROVIDER_ARCHIVE_URI')
    }
