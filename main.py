import logging
import os
import shutil
from distutils.util import strtobool

import click
import urllib3
from tqdm import tqdm
from urllib3.exceptions import InsecureRequestWarning

from utils import (
    dump_index,
    get_es,
    get_index_list,
    ingest,
    get_dump_path,
)

urllib3.disable_warnings(InsecureRequestWarning)

success_logger = logging.getLogger("successful_indices")
failure_logger = logging.getLogger("failed_indices")
success_file_handler = logging.FileHandler(f"./logs/success", mode="a")
failure_file_handler = logging.FileHandler(f"./logs/failure", mode="a")
success_logger.setLevel(logging.DEBUG)
failure_logger.setLevel(logging.DEBUG)
success_logger.handlers = [success_file_handler]
failure_logger.handlers = [failure_file_handler]


@click.group()
def cli():
    pass


@cli.command()
@click.argument("index")
@click.option("--source_hosts", default=os.getenv("ES_SOURCE_HOSTS", "localhost:9200"))
@click.option("--source_username", default=os.getenv("ES_SOURCE_USER", None))
@click.option("--source_password", default=os.getenv("ES_SOURCE_PASSWORD", None))
@click.option("--source_secured", default=os.getenv("ES_SOURCE_SECURED", False))
@click.option("--read_timeout", default=os.getenv("ES_READ_TIMEOUT", 60))
@click.option("--read_size", default=os.getenv("ES_READ_SIZE", 100))
@click.option("--max_slices", default=os.getenv("ES_MAX_SLICES", 5))
def dump(
    index,
    source_hosts,
    source_username,
    source_password,
    source_secured,
    read_timeout,
    read_size,
    max_slices,
):
    client = get_es(
        source_hosts, source_secured, read_timeout, source_username, source_password
    )
    dump_index(
        client,
        index,
        max_slices,
        read_size,
        read_timeout,
    )


@cli.command()
@click.argument("index_list_path")
@click.option("--source_hosts", default=os.getenv("ES_SOURCE_HOSTS", "localhost:9200"))
@click.option("--target_hosts", default=os.getenv("ES_TARGET_HOSTS", "localhost:9200"))
@click.option(
    "--source_secured", default=strtobool(os.getenv("ES_SOURCE_SECURED", "False"))
)
@click.option(
    "--target_secured", default=strtobool(os.getenv("ES_TARGET_SECURED", "False"))
)
@click.option("--source_username", default=os.getenv("ES_SOURCE_USER", None))
@click.option("--source_password", default=os.getenv("ES_SOURCE_PASSWORD", None))
@click.option("--target_username", default=os.getenv("ES_TARGET_USER", None))
@click.option("--target_password", default=os.getenv("ES_TARGET_PASSWORD", None))
@click.option("--read_timeout", default=int(os.getenv("ES_READ_TIMEOUT", 60)))
@click.option("--read_size", default=int(os.getenv("ES_READ_SIZE", 100)))
@click.option("--max_slices", default=int(os.getenv("ES_MAX_SLICES", 5)))
@click.option("--write_timeout", default=int(os.getenv("ES_WRITE_TIMEOUT", 60)))
@click.option("--write_size", default=int(os.getenv("ES_WRITE_SIZE", 100)))
@click.option(
    "--write_retain_ids", default=strtobool(os.getenv("ES_WRITE_RETAIN_IDS", "True"))
)
@click.option("--write_parallelism", default=int(os.getenv("ES_WRITE_PARALLELISM", 1)))
@click.option(
    "--write_max_chunk_size",
    default=int(os.getenv("ES_WRITE_CHUNK_SIZE", 100 * 1000 * 1000)),
)
@click.option(
    "--delete_staged_files",
    default=strtobool(os.getenv("ES_DELETE_STAGED_FILES", "True")),
)
def copy(
    index_list_path,
    source_hosts,
    target_hosts,
    source_username,
    source_password,
    source_secured,
    target_secured,
    target_username,
    target_password,
    read_timeout,
    read_size,
    max_slices,
    write_timeout,
    write_size,
    write_retain_ids,
    write_parallelism,
    write_max_chunk_size,
    delete_staged_files,
):
    source_client = get_es(
        source_hosts, source_secured, read_timeout, source_username, source_password
    )
    target_client = get_es(
        target_hosts, target_secured, write_timeout, target_username, target_password
    )
    index_list = get_index_list(source_client, target_client, index_list_path)
    with tqdm(total=len(index_list), desc="Processing indices", position=0) as pbar:
        for index in index_list:
            try:
                dump_index(
                    source_client,
                    index,
                    max_slices,
                    read_size,
                    read_timeout,
                    tqdm_position=1,
                )
                index_path = get_dump_path(index).as_posix()
                index_file_glob = index_path + "/*"
                ingest(
                    target_client,
                    index_file_glob,
                    write_retain_ids,
                    write_timeout,
                    write_size,
                    write_parallelism,
                    index,
                    write_max_chunk_size,
                    tqdm_position=1,
                )
                if delete_staged_files:
                    shutil.rmtree(index_path)
                success_logger.info(index)
            except Exception as e:
                click.echo(f"Failed to process index {str(e)}", err=True)
                failure_logger.exception(index)
            pbar.update(1)


if __name__ == "__main__":
    cli()
