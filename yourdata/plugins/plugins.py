# from yourdata.plugins import plugins_base
import plugins_base
import dask.bag as db


class FileSys(plugins_base.DataSourceBase):
    """ Reads data from local filesystem utilizing multicore parallelism if possible
    """

    def read_text(self, f):
        self.bag = db.read_text(f)
        return self.bag

    def load(self, filename):
        self.lines = self.read_text(filename)
        return self.lines


class PlainText(plugins_base.DataTypeConversionBase):
    """ Performs conversion to plain text via python str function.
    """
    def convert(self, line):
        import json
        try:
            self.payload = line.map(str)
            #self.payload = db.map(json.loads, line)
        except:
            self.payload = 'None'
        return self.payload

class Json(plugins_base.DataTypeConversionBase):
    """ Performs conversion to plain json
    """
    def convert(self, line):
        import json
        try:
            self.payload = db.map(json.loads, line)
        except:
            self.payload = 'None'
        return self.payload

class Bytes(plugins_base.DataTypeConversionBase):
    """ Performs conversion to bytes "encode()" function. Requires proper codec as additional named parameter). Default is "utf-8"
    """
    def convert(self, line, codec='utf-8'):
        try:
            self.payload = line.map(encode,codec)
        except:
            self.payload = 'Conversion error'
        return self.payload

class NoKey(plugins_base.KeyMethodBase):
    """ Generates empty key for record to be written to the chain.
    """
    def generate_key(self, line):
        return ""


class Zstd(plugins_base.CompressionBase):
    """ Performs compression based on zstandard library.
    """
    def compress_line(self, l):
        import zstd
        cctx = zstd.ZstdCompressor(write_content_size=True)
        xcompressed = cctx.compress(l.encode())
        return xcompressed

    def compress (self, bag):
        return bag.map(self.compress_line)

class NoCompress(plugins_base.CompressionBase):
    """ Performs no compression
    """
    def compress(self, line):
        return line
