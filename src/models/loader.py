class Loader:
    def __init__(self, _id='', start_extracting_block_number=None, last_updated_at_block_number=None):
        self.id = _id
        self.start_extracting_block_number = start_extracting_block_number
        self.last_updated_at_block_number = last_updated_at_block_number

    def to_dict(self):
        return {
            'id': self.id,
            'startExtractingAtBlockNumber': self.start_extracting_block_number,
            'lastUpdatedAtBlockNumber': self.last_updated_at_block_number
        }

    def from_dict(self, data):
        self.id = data['id']
        self.start_extracting_block_number = data.get('startExtractingAtBlockNumber')
        self.last_updated_at_block_number = data.get('lastUpdatedAtBlockNumber')
