import optparse

from snakepit import create, connect, directory

def create_hive():
    parser = optparse.OptionParser(
        usage='%prog HIVE_URI',
        )
    (opts, args) = parser.parse_args()
    try:
        (hive_uri,) = args
    except ValueError:
        parser.error('missing arguments')
    hive_metadata = create.create_hive(
        hive_uri=hive_uri,
        )
    hive_metadata.bind.dispose()


def create_dimension():
    parser = optparse.OptionParser(
        usage='%prog HIVE_URI DIMENSION_NAME DB_TYPE [DIRECTORY_URI]',
        )
    (opts, args) = parser.parse_args()
    try:
        (hive_uri, dimension_name, db_type, directory_uri) = args
    except ValueError:
        try:
            (hive_uri, dimension_name, db_type) = args
        except ValueError:
            parser.error('missing arguments')
        directory_uri = hive_uri

    if db_type not in directory.DB_TYPES:
        parser.error('Unknown DB_TYPE: %r' % db_type)

    directory_metadata = create.create_primary_index(
        directory_uri=directory_uri,
        dimension_name=dimension_name,
        db_type=db_type,
        )
    directory_metadata.bind.dispose()

    hive_metadata = connect.get_hive(hive_uri)
    create.create_dimension(
        hive_metadata=hive_metadata,
        dimension_name=dimension_name,
        directory_uri=directory_uri,
        db_type=db_type,
        )
    hive_metadata.bind.dispose()


def create_node():
    parser = optparse.OptionParser(
        usage='%prog HIVE_URI DIMENSION_NAME NODE_NAME NODE_URI',
        )
    (opts, args) = parser.parse_args()
    try:
        (hive_uri, dimension_name, node_name, node_uri) = args
    except ValueError:
        parser.error('missing arguments')

    hive_metadata = connect.get_hive(hive_uri)
    create.create_node(
        hive_metadata,
        dimension_name,
        node_name,
        node_uri,
        )
    hive_metadata.bind.dispose()


# TODO
# create-resource HIVE_URI RESOURCE_NAME
# create-secondary HIVE_URI RESOURCE_NAME COLUMN_NAME
