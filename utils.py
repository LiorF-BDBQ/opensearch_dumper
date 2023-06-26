import json
import pathlib
import time
from glob import glob
from mmap import mmap, PROT_READ
from multiprocessing.pool import ThreadPool
from typing import Tuple

import click
from opensearchpy import OpenSearch, helpers, NotFoundError
from tqdm import tqdm

PROCESS_START = int(time.time())


class ReconciliationError(Exception):
    pass


def get_es(hosts, secured, read_timeout, username, password):
    return OpenSearch(
        hosts=hosts.split(","),
        read_timeout=read_timeout,
        http_auth=(username, password) if username and password else None,
        use_ssl=secured,
        verify_certs=False,
    )


def dump_index(
    client: OpenSearch,
    index: str,
    max_slices: int,
    read_size: int,
    read_timeout: int,
    tqdm_position: int = 0,
):
    # Refresh the index before getting count of docs
    number_of_shards = get_num_of_shards(client, index)
    slices = min(max_slices, number_of_shards)
    client.indices.refresh(index=index)
    total = get_doc_count(client, index)
    with tqdm(desc=f"Dumping {index}", total=total, position=tqdm_position) as pbar:
        args = [
            (client, index, read_size, read_timeout, i, slices, pbar)
            for i in range(slices)
        ]
        with ThreadPool(max_slices) as pool:
            pool.starmap(dump_slice, args)
    total_in_files = 0
    time.sleep(2)
    for file in get_dump_path(index).glob("*.jsonl"):
        total_in_files += get_row_count(file)
    if total_in_files != total:
        raise ReconciliationError(
            f"Count of docs in index({total}) didn't match rows in files {total_in_files}"
        )
    return True


def get_doc_count(client, index):
    return int(client.count(index=index)["count"])


def get_num_of_shards(client, index):
    settings = client.indices.get_settings(index=index)[index]["settings"]
    number_of_shards = int(settings["index"]["number_of_shards"])
    return number_of_shards


def get_dump_path(index):
    return pathlib.Path(f"{index}_{PROCESS_START}")


def dump_slice(
    client: OpenSearch,
    index: str,
    size: int,
    read_timeout: int,
    slice_id: int,
    max_slices: int,
    pbar: tqdm,
):
    query = {"slice": {"id": slice_id, "max": max_slices}} if max_slices > 1 else None
    folder = get_dump_path(index)
    folder.mkdir(exist_ok=True)
    with open(folder / f"{slice_id}_dump.jsonl", mode="w") as out:
        try:
            for d in helpers.scan(
                client,
                index=index,
                query=query,
                size=size,
                scroll=f"{read_timeout}s",
                raise_on_error=True,
                preserve_order=False,
                request_timeout=read_timeout,
                timeout=f"{read_timeout}s",
            ):
                del d["_score"]
                del d["sort"]
                out.write(("%s\n" % json.dumps(d, ensure_ascii=False)))
                pbar.update(1)
        except Exception as e:
            click.echo(f"Error dumping slice {index} {slice_id}- {str(e)}", err=True)
            raise e
    return True


def ingest_slice(
    client: OpenSearch,
    source_file: str,
    retain_ids: bool,
    write_timeout: int,
    write_size: int,
    max_chunk_bytes: int,
    new_index_name: str,
    partition: Tuple[int, int],
    pbar: tqdm,
):
    for chunk in process_chunks(
        partition, source_file, write_size, retain_ids, new_index_name
    ):
        num_actions = len(chunk)
        helpers.bulk(
            client,
            chunk,
            chunk_size=num_actions,
            max_chunk_bytes=max_chunk_bytes,
            request_timeout=write_timeout,
            max_retries=3,  # handling 429 errors only, any failures would raise.
        )
        pbar.update(num_actions)
    return True


def process_chunks(partition, source_file, write_size, retain_ids, new_index_name):
    def process_doc(doc: dict):
        if not retain_ids:
            del doc["_id"]
        if new_index_name:
            doc["_index"] = new_index_name
        return doc

    with open(source_file, "r", encoding="utf-8") as f:
        f.seek(partition[0])
        position = partition[0]
        while position < partition[1]:
            actions = []
            while len(actions) < write_size and position < partition[1]:
                actions.append(process_doc(json.loads(f.readline())))
                position = f.tell()
            yield actions


def ingest_file(
    client: OpenSearch,
    source_file: str,
    retain_ids: bool,
    write_timeout: int,
    write_size: int,
    write_parallelism: int,
    max_chunk_size: int,
    new_index_name: str,
    bar_position: int = 0,
) -> bool:
    file_len, partition_offsets = split_file_by_rows(source_file, write_parallelism)
    with tqdm(
        desc=f"Writing from {source_file}", total=file_len, position=bar_position
    ) as pbar:
        args = [
            (
                client,
                source_file,
                retain_ids,
                write_timeout,
                write_size,
                max_chunk_size,
                new_index_name,
                partition,
                pbar,
            )
            for partition in partition_offsets
        ]
        with ThreadPool(len(partition_offsets)) as pool:
            pool.starmap(ingest_slice, args)
            return file_len


def reconcile(client, expected_doc_count, index_name):
    client.indices.refresh(index=index_name)
    doc_count = get_doc_count(client, index_name)
    if doc_count != expected_doc_count:
        raise ReconciliationError(f"Doc count for index {index_name} didn't match")


def ingest(
    client: OpenSearch,
    files_pattern: str,
    retain_ids: bool,
    write_timeout: int,
    write_size: int,
    write_parallelism: int,
    new_index_name: str,
    max_chunk_size: int,
    tqdm_position: int = 0,
):
    files = glob(files_pattern)
    total_count = 0
    for file in files:
        file_len = ingest_file(
            client,
            file,
            retain_ids,
            write_timeout,
            write_size,
            write_parallelism,
            max_chunk_size,
            new_index_name,
            bar_position=tqdm_position,
        )
        total_count += file_len
    reconcile(client, total_count, new_index_name)
    return True


def get_row_count(source_file):
    with open(source_file, "r") as f:
        try:
            buf = mmap(f.fileno(), 0, prot=PROT_READ)
            lines = 0
            readline = buf.readline
            while readline():
                lines += 1
            return lines
        except Exception as e:
            click.echo(f"Error reading {source_file}, got error {e}")
            raise ReconciliationError("Cannot get row count from source file")


def split_file_by_rows(source_file, num_parts):
    row_count = get_row_count(source_file)
    offsets = []
    start = 0
    # If we cannot split the
    # number into exactly 'N' parts, just create 1 partition
    if row_count < num_parts:
        offsets.append((start, row_count))

    # If x % n == 0 then the minimum
    # difference is 0 and all
    # numbers are x / n
    elif row_count % num_parts == 0:
        for i in range(num_parts):
            length = row_count // num_parts
            offsets.append((start, start + length - 1))
            start = start + length

    else:
        # upto parts-(len % parts) the values
        # will be len / parts
        # after that the values
        # will be len / parts + 1
        zp = num_parts - (row_count % num_parts)
        pp = row_count // num_parts
        for i in range(num_parts):
            if i >= zp:
                length = pp + 1
            else:
                length = pp
            offsets.append((start, start + length - 1))
            start = start + length
    offsets = [get_offsets_from_rows(source_file, offset) for offset in offsets]
    return row_count, offsets


def get_offsets_from_rows(source_file, offsets):
    with open(source_file, "r") as f:
        # Read into empty buffer
        buf = mmap(f.fileno(), 0, prot=PROT_READ)
        lines = 0
        readline = buf.readline
        while lines < offsets[0] and readline():
            lines += 1
        # We got to the start line
        start_offset = buf.tell()
        while readline() and lines < offsets[1]:
            lines += 1
        end_offset = buf.tell()
        return start_offset, end_offset


def get_index_list(
    source_client: OpenSearch, target_client: OpenSearch, path_to_index_list: str
):
    with open(path_to_index_list, "r") as f:
        indices = [s for s in [s.strip() for s in f.read().split("\n")] if s]
    # Make sure all indices exist in source
    for i in indices:
        index = source_client.indices.get(i)
        if not index:
            raise NotFoundError()
        try:
            index = target_client.indices.get(i)
            if index:
                raise Exception(f"Index {i} already exists in target cluster")
        except NotFoundError:
            pass
    return indices
