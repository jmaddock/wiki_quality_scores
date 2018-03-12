import subprocess
import utils

## Class for decompressing, iterating through, and re-compressing wiki xml dumps
## Will not decompress/recompress in debug mode
class DumpHandler(object):
    def __init__(self,f_in):
        self.f_in = f_in
        self.uncompressed = f_in.rsplit('.7z', 1)[0]
        self.dump = None
        self.base_dir = f_in.rsplit('/', 1)[0]

    # create an iterator for a dump that has already been decompressed
    # depreciated
    def open_dump(self):
        utils.log('opening file: %s' % self.f_in)
        self.dump = mwxml.Dump.from_file(self.db_path)
        return self.dump

    # decompress a dump from a .7z archive
    def decompress(self):
        utils.log('decompressing file: %s' % self.f_in)
        subprocess.call(['7z','x',self.f_in,'-o' + self.base_dir])

    # remove the decompressed dump after processing
    def remove_dump(self):
        self.dump = None
        utils.log('removing file: %s' % self.uncompressed)
        subprocess.call(['rm',self.uncompressed])

    # convenience method for decompressing and opening a dump
    def process_dump(self):
        if os.path.exists(self.uncompressed):
            self.remove_dump()
        self.decompress()
        self.open_dump()
        return self.dump