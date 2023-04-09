from multiprocessing import Pool

import click
from tqdm import tqdm
from opensearchpy import helpers, NotFoundError, OpenSearch
import json
import gzip


def get_es(hosts, read_timeout):
    return OpenSearch(hosts=hosts,
                      read_timeout=read_timeout)


def dump_slice(hosts, index, size, scroll_timeout, read_timeout, slice_id, max_slices):
    es_source = get_es(hosts, read_timeout)
    query = {"slice": {"id": slice_id, "max": max_slices}}
    with gzip.open(index + '_' + str(slice_id) + '_dump.jsonl.gz', mode='wb') as out:
        try:
            for d in tqdm(
                    helpers.scan(
                        es_source,
                        index=index,
                        query=query,
                        size=size,
                        scroll=scroll_timeout,
                        raise_on_error=True,
                        preserve_order=False,
                        request_timeout=read_timeout
                    )
            ):
                out.write(("%s\n" % json.dumps(d['_source'], ensure_ascii=False)).encode(encoding='UTF-8'))
        except NotFoundError:
            click.echo(f'Error dumping index {index}: Not Found', err=True)
            return False
    return True


@click.group()
def cli():
    pass


@cli.command()
@click.argument('index')
@click.option('--hosts')
@click.option('--scroll_timeout', default=u'1m')
@click.option('--read_timeout', default=60)
@click.option('--size', default=1000)
@click.option('--max_slices', default=1)
def dump(hosts, index, size, scroll_timeout, read_timeout, max_slices):
    pool = Pool(max_slices)
    args = [(hosts, index, size, scroll_timeout, read_timeout, i, max_slices) for i
            in range(max_slices)]
    pool.starmap(dump_slice, args)


if __name__ == '__main__':
    cli()
