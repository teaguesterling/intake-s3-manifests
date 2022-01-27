# -*- coding: utf-8 -*-
from . import __version__
from intake.source.base import DataSource, Schema

import json
import dask.dataframe as dd
from datetime import datetime, timedelta
import s3fs


class S3ManifestSource(DataSource):
    """Common behaviours for plugins in this repo"""
    name = 's3_manifest'
    version = __version__
    container = 'dataframe'
    partition_access = True

    def __init__(self, manifest_bucket, source_bucket, config_id, manifest_date='latest', s3_prefix='s3://', s3_manifest_kwargs=None,
                 extract_key_regex=None, s3_anon=True, **kwargs):
        """
        Parameters
        ----------
        manifest_bucket : str
            The S3 bucket which contains the manifest files.
        source_bucket : str
            The S3 bucket for which you want to load the manifest.
        config_id : str
            The S3 inventory config ID.
            See https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-inventory.html#storage-inventory-location
        manifest_date: str
            The date of the manifest you wish to load in for format `YYYY-MM-DD`. Defaults to `latest` which will
            load the most recent manifest.
        s3_prefix: str
            The prefix for accessing S3. Defaults to the `s3://` protocol. If you are using fuse for example you may
            want to set this to the mount point of the bucket.
        s3_manifest_kwargs : dict
            Any further arguments to pass to Dask's read_csv (such as block size)
            or to the `CSV parser <https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_csv.html>`_
            in pandas (such as which columns to use, encoding, data-types)
        extract_key_regex: string
            Pandas is able to extract information from a composite string using regular expressions. If the objects
            in your bucket follow a strict naming convension you can provide a regular expression with named groups
            to extract the information from the key into separate columns.
            e.g if your bucket contains images from an ecommerce website they may follow the format
            `<category>_<item name>_<item_id>.jpg` which you could extract using the expression
            `(?P<Category>.*)_(?P<Name>.*)_(?P<ID>..).jpg`.
        s3_anon: bool
            When reading manifests from S3 (s3_prefix = "s3://") then do so with out sending credentials. Default is True.
        """
        super().__init__(**kwargs)
        self._manifest_bucket = manifest_bucket
        self._source_bucket = source_bucket
        self._manifest_date = manifest_date
        self._config_id = config_id
        self._s3_anon = s3_anon
        if self._manifest_date == 'latest':
            self._manifest_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        self._s3_prefix = s3_prefix
        self._urlpath = '{prefix}{manifest_bucket}/{source_bucket}/{config_id}/{date}/manifest.json'.format(
            prefix=self._s3_prefix,
            manifest_bucket=self._manifest_bucket,
            source_bucket=self._source_bucket,
            config_id=self._config_id,
            date=self._manifest_date)
        self._extract_key_regex = extract_key_regex
        if self._extract_key_regex is not None:
            self._extract_key_regex = r'%s' % extract_key_regex
        self._s3_manifest_kwargs = s3_manifest_kwargs or {}
        self._dataframe = None

    def _open_manifest(self, url):
        if self._s3_prefix.split('/')[0] == 's3:':
            # s3 :- use `s3fs`
            fs = s3fs.S3FileSystem(anon=self._s3_anon)
            return fs.open(url, 'rb')
        else:
            # other :- use `open`
            return open(url, 'rb')

    def _open_dataset(self):

        with self._open_manifest(self._urlpath) as f:
            manifest_meta = json.load(f)
            manifests = [file['key'] for file in manifest_meta['files']]

            partitions = [dd.read_csv('{prefix}{bucket}/{key}'.format(prefix=self._s3_prefix, bucket=manifest_meta['sourceBucket'], key=manifest),
                                      names=['Bucket', 'Key', 'Size', 'Created'], compression='gzip', blocksize=None) for manifest in manifests]
            df = dd.concat(partitions)
            df = df[~df['Key'].str.contains("/{source_bucket}/{config_id}/".format(source_bucket=self._source_bucket, config_id=self._config_id))]
            if self._extract_key_regex is not None:
                metadata = df.Key.str.extract(self._extract_key_regex, expand=False)
                df = dd.concat([df, metadata], axis=1)

        self._dataframe = df

    def _get_schema(self):
        if self._dataframe is None:
            self._open_dataset()

        dtypes = self._dataframe._meta.dtypes.to_dict()
        dtypes = {n: str(t) for (n, t) in dtypes.items()}
        return Schema(datashape=None,
                      dtype=dtypes,
                      shape=(None, len(dtypes)),
                      npartitions=self._dataframe.npartitions,
                      extra_metadata={})

    def _get_partition(self, i):
        self._get_schema()
        return self._dataframe.get_partition(i).compute()

    def read(self):
        self._get_schema()
        return self._dataframe.compute()

    def to_dask(self):
        self._get_schema()
        return self._dataframe

    def _close(self):
        self._dataframe = None
