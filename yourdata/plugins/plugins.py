# from yourdata.plugins import plugins_base
import plugins_base
import dask.bag as db


class Filesys(plugins_base.DataSourceBase):
    """ Reads data from local filesystem utilizing multicore parallelism if possible
    """
    def load(self, filename):
        self.lines = db.read_text(filename)
        return self.lines
