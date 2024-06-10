from multithread_processing.base_job import BaseJob

from src.utils.logger_utils import get_logger

logger = get_logger('Calculate Optimized Job')

class CalculateOptimizedRange(BaseJob):
    def __init__(self, chain_id, pairs):
        self.chain_id = chain_id
        self.pairs = pairs
        super.__init__()