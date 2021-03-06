#!/usr/bin/python
from configparser import ConfigParser


def config_db(filename='./database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db_params = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db_params[param[0]] = param[1]
    else:
        raise Exception('Section {} not found in the {} file'.format(section, filename))

    return db_params
