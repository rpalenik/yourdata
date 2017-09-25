# from yourdata.plugins import plugins_base
import plugins_base
try:
    import core as ycore
except:
    import yourdata.core as ycore
import dask
import dask.bag as db
import dask.dataframe as dd
from dask import delayed


class FileSys(plugins_base.DataSourceBase):
    """ Reads data from local filesystem utilizing multicore parallelism
     if possible
    """

    def read_text(self, f):
        self.bag = db.read_text(f)
        return self.bag

    def read_csv(self, f, target_format):
        self.frame = dd.read_csv(f)
        if target_format == 'bag':
            self.ret = self.frame.to_bag()
        else:
            self.ret = self.frame
        return self.ret

    def load(self, path, source_format, target_format):
        if source_format == 'rawtxt':
            self.lines = self.read_text(path)
        else:
            self.lines = self.read_csv(path, target_format)
        return self.lines

    def write(self, filename):
        return False


class Url(plugins_base.DataSourceBase):
    """ Reads data from local filesystem utilizing multicore parallelism
     if possible
    """

    def get_url_list(self, url, headers):
        import requests
        headers['Accept'] = 'application/json'
        params = {'as_list': 'true', 'as_access_list': 'false', 'refresh': 'true',
                  'include_folders': 'true', 'include_files': 'true', 'full_tree': 'false', 'zip': 'false'}
        result = requests.get(url, headers=headers, params=params)
        return result.json()

    def get_url(self, url, path, headers, data):
        import requests
        handler = requests.get(
            url + '/' + path, headers=headers, data=data, stream=True)
        return handler

    def read_text(self, url, path, headers, data):
        import fnmatch
        f_list = self.get_url_list(url, headers)
        get_list = fnmatch.filter([y for y in f_list['resource']], path)
        lists = []
        for file in get_list:
            handler = dask.delayed(self.get_url)(url, file, headers, data)
            lst = dask.delayed(handler.iter_lines())
            lists.append(lst)
        return lists

    def read_csv(self, url, path, headers, data):
        import csv
        dels = self.read_text(url, path, headers, data)
        new_dels = []
        for x in dels:
            new_dels.append(delayed(list(csv.DictReader(x.compute()))))
        return new_dels

    def load(self, url, path, headers, data, source_format, target_format):

        if source_format == 'rawtxt':
            self.output = self.read_text(url, path, headers)
        else:
            self.output = self.read_csv(url, path, headers)

        if target_format == "frame":
            result = dd.from_delayed(self.output)
        else:
            result = db.from_delayed(self.output)
        return self.output

    def write(self, filename):
        return False


class BlockChain(plugins_base.DataSourceBase):
    """ Reads/writes data from/to Blockchain ledger utilizing multicore parallelism if possible
    """

    def load(self, dataset, subset, key, count, start):
        import binascii
        rpc_connection = dask.delayed(ycore.create_rpc_conn)(dataset)
        if count == -1:
            count = dask.delayed(rpc_connection.liststreams)(subset)[
                0]['items']
        else:
            count = dask.delayed(count)
        start = dask.delayed(start)

        xset = dask.delayed(rpc_connection.liststreamkeyitems)(
            subset, key, False, count, start)
        xbag = db.from_delayed(xset).pluck('data').map(binascii.unhexlify)
        # xbag = xbag.map(binascii.unhexlify)
        return xbag

    def write_line(self, l, rpc_connection, key_driver, dataset, subset):
        import binascii
        try:
            key = key_driver.generate_key(l)
            if type(l) != bytes:
                l = l.encode()
            payload = binascii.hexlify(l)
            rpc_connection.publish(subset, key, payload.decode('utf-8'))
            return l  # returns unchanged line for further processing
        except:
            raise

    def write(self, bag, key_method, dataset, subset):
        try:
            rpc_connection = dask.delayed(ycore.create_rpc_conn(dataset))
            drv_key = dask.delayed(ycore.get_driver('plugins.key', key_method))
            # payload = db.map(self.write_line, bag, rpc_connection,
            #            drv_key, dataset, subset)
            return bag.map(self.write_line, rpc_connection, drv_key, dataset, subset)

        except:
            raise


class PlainText(plugins_base.DataTypeConversionBase):
    """ Performs conversion to plain text via python str function.
    """

    def convert(self, line):
        # import json
        try:
            self.payload = line.map(str)
            # self.payload = db.map(json.loads, line)
        except:
            self.payload = 'None'
        return self.payload


class Json(plugins_base.DataTypeConversionBase):
    """ Performs conversion to plain json
    """
    import json

    def convert_line(self, l):
        try:
            res = json.loads(l)
        except:
            res = {'result': 'Error converting to JSON'}
        return res

    def convert(self, line):
        self.payload = db.map(self.convert_line, line)
        return self.payload


class Bytes(plugins_base.DataTypeConversionBase):
    """ Performs conversion to bytes "encode()" function. Requires
    proper codec as additional named parameter). Default is "utf-8"
    """

    def convert(self, line, codec='utf-8'):
        try:
            self.payload = line.map(encode, codec)
        except:
            self.payload = 'Conversion error'
        return self.payload


class NoKey(plugins_base.KeyMethodBase):
    """ Generates empty key for record to be written to the chain.
    """

    def generate_key(self, line):
        return "ABC"


class Zstd(plugins_base.CompressionBase):
    """ Performs compression based on zstandard library.
    """

    def compress_line(self, l):
        import zstd
        cctx = zstd.ZstdCompressor(write_content_size=True)
        xcompressed = cctx.compress(l.encode())
        return xcompressed

    def decompress_line(self, l):
        import zstd
        dctx = zstd.ZstdDecompressor()
        xdecompressed = dctx.decompress(l)
        return xdecompressed

    def compress(self, bag):
        return bag.map(self.compress_line)

    def decompress(self, bag):
        return bag.map(self.decompress_line)


class NoCompress(plugins_base.CompressionBase):
    """ Performs no compression
    """

    def compress(self, line):
        return line
