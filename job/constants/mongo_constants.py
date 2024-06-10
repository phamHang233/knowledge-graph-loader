class MongoDBCollections:
    wallets = 'wallets'
    projects = 'projects'
    smart_contracts = 'smart_contracts'
    relationships = 'relationships'
    call_smart_contracts = 'call_smart_contracts'
    interactions = 'interactions'
    abi = 'abi'
    configs = 'configs'
    multichain_wallets = 'multichain_wallets'
    multichain_wallets_credit_scores = 'multichain_wallets_credit_scores_v3'

    is_part_ofs = 'is_part_ofs'


class MongoDBKeys:
    project_overview = 'project_overview'


class KnowledgeGraphModel:
    edge_definitions = [
        {
            'edge_collection': MongoDBCollections.relationships,
            'from_vertex_collections': [
                MongoDBCollections.wallets,
                MongoDBCollections.projects,
                MongoDBCollections.smart_contracts
            ],
            'to_vertex_collections': [
                MongoDBCollections.wallets,
                MongoDBCollections.projects,
                MongoDBCollections.smart_contracts
            ],
        }
    ]


class MongoDBIndex:
    smart_contract_tags = 'smart_contract_tags'
    smart_contract_address = 'smart_contract_address'
    smart_contract_chain = 'smart_contract_chain'
    relationship_type = 'relationship_type'
    relationship_from_type = 'relationship_from_type'
    relationship_ttl = 'relationship_ttl'


class WalletLabels:
    elite = 'elite'
    new_elite = 'newElite'
    target = 'target'
    new_target = 'newTarget'


class MongoDBDexCollections:
    dexes = 'dexes'
    pairs = 'pairs'
    cexes = 'cexes'
    nfts = 'nfts'
    tokens = 'tokens'
    tickers = 'tickers'
    exchanges = 'exchanges'

    configs = 'configs'


class MongoDBdYdXCollections:
    wallets = 'wallets'
    smart_contracts = 'smart_contracts'
    events = 'events'
    configs = 'configs'
