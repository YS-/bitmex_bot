import logging
import os
from datetime import datetime


def create_dir(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)


def get_basename(path):
    return os.path.basename(path)


def get_file_dir(file_name):
    return os.path.dirname(os.path.abspath(file_name))


def get_base_dir():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_current_executing_service_path():
    delimiter = "\x00"
    pid = os.getpid()
    with open(os.path.join('/proc', str(pid), 'cmdline'), 'rb') as f:
        file_data = f.read().decode("ascii")
        data = file_data.split(delimiter)
        attributes = [attr for attr in data if attr.endswith(".py")]
        return attributes[0] if attributes else "tmp"


def get_logger(name):
    logger = logging.getLogger(name)

    base_dir = get_base_dir()
    log_directory_path = os.path.join(base_dir, "logs")
    create_dir(log_directory_path)

    service_name = get_basename(os.path.splitext(get_current_executing_service_path())[0])

    hdlr = logging.FileHandler(os.path.join(log_directory_path, "%s.log" % service_name))
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(filename)s(%(lineno)d) %(message)s')

    hdlr.setFormatter(formatter)

    logger.addHandler(hdlr)
    logger.setLevel(logging.DEBUG)

    return logger


def convert_date_string_to_timestamp(datetime_string):
    try:
        datetime_field = datetime.strptime(datetime_string, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        datetime_field = datetime.strptime(datetime_string, "%Y-%m-%dT%H:%M:%S")
    return datetime_field.timestamp() * 1000


def get_formatted_exchange_result(*args):
    return "~".join([str(arg) for arg in args])
