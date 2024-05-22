from src.databases.mongodb_klg import MongoDB
from src.exporters.mongodb_exporter import MongoDBExporter


def create_entity_db_and_exporter(output=None, exporter_type=None):
    exporter_type = determine_item_exporter_type(output, exporter_type)

    if exporter_type == ExporterType.MONGODB:
        db = MongoDB(output)
        exporter = MongoDBExporter(db=db)
    else:
        raise ValueError('Unable to determine item exporters type for output ' + output)

    return db, exporter


# def create_klg_db_and_exporter(output=None, exporter_type=None):
#     exporter_type = determine_item_exporter_type(output, exporter_type)
#
#     if exporter_type == ExporterType.ARANGODB:
#         db = KnowledgeGraph(output)
#         exporter = KLGExporter(db=db)
#     else:
#         raise ValueError('Unable to determine item exporters type for output ' + output)
#
#     return db, exporter


def determine_item_exporter_type(output=None, exporter_type=None):
    if output is None:
        type_ = exporter_type
    elif output == 'console':
        type_ = ExporterType.CONSOLE
    elif output.startswith('arangodb'):
        type_ = ExporterType.ARANGODB
    elif output.startswith('mongodb'):
        type_ = ExporterType.MONGODB
    else:
        type_ = ExporterType.UNKNOWN

    if (exporter_type is not None) and (type_ != exporter_type):
        raise ValueError('Exporter type mismatch output')

    return type_


class ExporterType:
    CONSOLE = 'console'
    ARANGODB = 'arangodb'
    MONGODB = 'mongodb'
    UNKNOWN = 'unknown'
